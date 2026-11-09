// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
//  https://nautechsystems.io
//
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
//  You may not use this file except in compliance with the License.
//  You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
// -------------------------------------------------------------------------------------------------

//! Resumes a Binance Spot user data stream after the venue ends it.
//!
//! Futures rotates its listen key and emits open-order status from the same recovery task.
//! Spot uses that sequence for Binance US, and the existing WebSocket API session logon plus
//! `userDataStream.subscribe` path for the global exchange. Both then replay order status so the
//! live engine can reconcile fills that arrived while the stream was down. Fill events themselves
//! are not replayed, because the execution engine would apply them again.

use std::{sync::Arc, time::Duration};

use ahash::AHashSet;
use anyhow::Context;
use nautilus_core::{DurationNanos, string::secret::SecretString, time::AtomicTime};
use nautilus_live::{ExecutionEventEmitter, SocketControlFactory};
use nautilus_model::{enums::OrderStatus, identifiers::AccountId, reports::OrderStatusReport};
use nautilus_network::websocket::TransportBackend;
use parking_lot::{Mutex, RwLock};

use super::{client::BinanceSpotWsTradingClient, messages::BinanceSpotWsTradingMessage};
use crate::{
    common::{
        consts::BINANCE_WS_HEARTBEAT_SECS, dispatch::WsDispatchState,
        urls::get_spot_user_stream_url,
    },
    config::BinanceInstrumentProviderConfig,
    spot::http::client::BinanceSpotHttpClient,
};

const RECOVERY_RETRY_INITIAL: Duration = Duration::from_secs(1);
const RECOVERY_RETRY_MAX: Duration = Duration::from_secs(30);
const ORDER_REPLAY_LOOKBACK_MINS: u64 = 60;

/// Why the user stream must be resumed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SpotUserStreamRecoveryKind {
    /// Legacy listen key reached its 60-minute lifetime.
    ListenKeyExpired,
    /// The venue closed the user data event stream.
    EventStreamTerminated,
}

/// How the current Spot session resumes its user stream.
#[derive(Debug)]
pub(crate) enum SpotUserStreamResume {
    /// Global Spot WebSocket API, which has no listen key.
    WsApi {
        /// Trading client that owns `session.logon` and `userDataStream.subscribe`.
        client: BinanceSpotWsTradingClient,
    },
    /// Binance US listen-key stream. The socket URL embeds the key, so recovery swaps it.
    UsListenKey {
        /// Slot the dispatch loop reads, replaced when the key rotates.
        user_data_client: Arc<Mutex<Option<BinanceSpotWsTradingClient>>>,
        /// Current listen key, also read by the keepalive task.
        listen_key: Arc<RwLock<Option<SecretString>>>,
        /// Bumped after the slot holds the replacement client.
        generation: tokio::sync::watch::Sender<u64>,
        /// API key and secret used to open the replacement socket.
        credentials: (SecretString, SecretString),
        /// Optional user-stream base URL override.
        base_url_ws: Option<String>,
        /// Optional proxy URL.
        proxy_url: Option<SecretString>,
        /// WebSocket transport selected by the execution client.
        transport_backend: TransportBackend,
        /// Factory for the replacement socket control.
        socket_factory: SocketControlFactory,
    },
}

/// Shared state for one Spot user-stream recovery driver.
#[allow(
    missing_debug_implementations,
    reason = "the Spot HTTP client has no Debug implementation"
)]
pub(crate) struct SpotUserStreamRecovery {
    /// HTTP client used to rotate keys and query order status.
    pub http_client: BinanceSpotHttpClient,
    /// Emits reconciled order status into the live execution engine.
    pub emitter: ExecutionEventEmitter,
    /// Account that owns the replayed reports.
    pub account_id: AccountId,
    /// Clock used to bound the order-history lookback.
    pub clock: &'static AtomicTime,
    /// Instrument scope applied to the open-order query.
    pub instrument_provider: BinanceInstrumentProviderConfig,
    /// Instruments already known to the user stream.
    pub dispatch_state: Arc<WsDispatchState>,
    /// When true, expired order reports are emitted as canceled.
    pub treat_expired_as_canceled: bool,
    /// Session-specific resume steps.
    pub resume: SpotUserStreamResume,
}

/// Returns the recovery trigger carried by `message`, if it ends the user stream.
pub(crate) fn recovery_kind(
    message: &BinanceSpotWsTradingMessage,
) -> Option<SpotUserStreamRecoveryKind> {
    match message {
        BinanceSpotWsTradingMessage::ListenKeyExpired { .. } => {
            Some(SpotUserStreamRecoveryKind::ListenKeyExpired)
        }
        BinanceSpotWsTradingMessage::EventStreamTerminated { .. } => {
            Some(SpotUserStreamRecoveryKind::EventStreamTerminated)
        }
        _ => None,
    }
}

/// Signals the recovery driver for a user-stream expiry.
///
/// Returns `true` when a recovery request was queued.
pub(crate) fn signal_user_stream_recovery(
    message: &BinanceSpotWsTradingMessage,
    tx: &tokio::sync::mpsc::UnboundedSender<SpotUserStreamRecoveryKind>,
) -> bool {
    let Some(kind) = recovery_kind(message) else {
        return false;
    };

    if tx.send(kind).is_err() {
        log::error!("Binance Spot user stream recovery task is not running");
        return false;
    }
    true
}

/// Runs until the channel closes, retrying each trigger until the stream is resumed.
pub(crate) async fn run_spot_user_stream_recovery(
    ctx: SpotUserStreamRecovery,
    mut rx: tokio::sync::mpsc::UnboundedReceiver<SpotUserStreamRecoveryKind>,
) {
    while let Some(kind) = rx.recv().await {
        while rx.try_recv().is_ok() {}
        recover_with_retry(&ctx, kind).await;
    }
}

async fn recover_with_retry(ctx: &SpotUserStreamRecovery, kind: SpotUserStreamRecoveryKind) {
    let mut delay = RECOVERY_RETRY_INITIAL;
    let mut attempt = 0u32;

    loop {
        attempt += 1;
        log::warn!("Recovering Binance Spot user stream after {kind:?}, attempt {attempt}");

        match resume_and_replay(ctx).await {
            Ok(()) => {
                log::info!("Binance Spot user stream recovery succeeded");
                return;
            }
            Err(e) => {
                log::warn!("Binance Spot user stream recovery attempt {attempt} failed: {e:#}");
                tokio::time::sleep(delay).await;
                delay = delay
                    .checked_mul(2)
                    .unwrap_or(RECOVERY_RETRY_MAX)
                    .min(RECOVERY_RETRY_MAX);
            }
        }
    }
}

async fn resume_and_replay(ctx: &SpotUserStreamRecovery) -> anyhow::Result<()> {
    match &ctx.resume {
        SpotUserStreamResume::WsApi { client } => {
            resume_ws_api_user_data(client).await?;
            replay_spot_user_stream(ctx).await
        }
        SpotUserStreamResume::UsListenKey { .. } => rotate_us_listen_key(ctx).await,
    }
}

async fn resume_ws_api_user_data(client: &BinanceSpotWsTradingClient) -> anyhow::Result<()> {
    client.mark_user_data_inactive();
    if let Err(e) = client.subscribe_user_data().await {
        log::warn!("Binance Spot user data resubscribe failed, re-authenticating: {e}");
        client
            .session_logon()
            .await
            .context("failed to re-authenticate Binance Spot user data after stream termination")?;
        client
            .subscribe_user_data()
            .await
            .context("failed to resubscribe Binance Spot user data after stream termination")?;
    }
    Ok(())
}

async fn rotate_us_listen_key(ctx: &SpotUserStreamRecovery) -> anyhow::Result<()> {
    let SpotUserStreamResume::UsListenKey {
        user_data_client,
        listen_key,
        generation,
        credentials,
        base_url_ws,
        proxy_url,
        transport_backend,
        socket_factory,
    } = &ctx.resume
    else {
        anyhow::bail!("Binance US listen key recovery requires the US user stream");
    };

    let response = ctx
        .http_client
        .inner()
        .create_listen_key()
        .await
        .context("failed to create Binance US listen key during user stream recovery")?;
    let new_key = response.into_listen_key();

    if let Err(e) = replay_spot_user_stream(ctx).await {
        close_listen_key(
            &ctx.http_client,
            &new_key,
            "Failed to close unused Binance US listen key after recovery replay failure",
        )
        .await;
        return Err(e);
    }

    let url = get_spot_user_stream_url(base_url_ws.as_deref(), new_key.expose_secret());
    let mut client = BinanceSpotWsTradingClient::new(
        Some(url),
        credentials.0.expose_secret().to_owned(),
        credentials.1.expose_secret().to_owned(),
        Some(BINANCE_WS_HEARTBEAT_SECS),
        *transport_backend,
    )
    .with_proxy(
        proxy_url
            .as_ref()
            .map(|value| value.expose_secret().to_owned()),
    )
    .with_socket_control(socket_factory.control("binance-spot-user-streams"));

    if let Err(e) = client.connect().await {
        close_listen_key(
            &ctx.http_client,
            &new_key,
            "Failed to close unused Binance US listen key after reconnect failure",
        )
        .await;
        return Err(e).context("failed to connect recovered Binance US user data stream");
    }

    client.mark_user_data_active();
    let previous = listen_key.read().clone();
    let old_client = user_data_client.lock().replace(client);
    *listen_key.write() = Some(new_key);
    let next = generation.borrow().saturating_add(1);
    if generation.send(next).is_err() {
        log::warn!("Binance US user data dispatch loop is not watching listen key rotation");
    }

    if let Some(mut old_client) = old_client {
        old_client.mark_user_data_inactive();
        if let Err(e) = old_client.disconnect().await {
            log::warn!("Failed to disconnect expired Binance US user stream: {e}");
        }
    }

    if let Some(previous) = previous {
        close_listen_key(
            &ctx.http_client,
            &previous,
            "Failed to close expired Binance US listen key",
        )
        .await;
    }
    Ok(())
}

async fn close_listen_key(http: &BinanceSpotHttpClient, listen_key: &SecretString, context: &str) {
    if let Err(e) = http
        .inner()
        .close_listen_key(listen_key.expose_secret())
        .await
    {
        log::warn!("{context}: {e}");
    }
}

async fn replay_spot_user_stream(ctx: &SpotUserStreamRecovery) -> anyhow::Result<()> {
    let mut reports = ctx
        .http_client
        .request_order_status_reports_scoped(
            ctx.account_id,
            None,
            None,
            None,
            true,
            None,
            Some(&ctx.instrument_provider),
        )
        .await
        .context("failed to replay open Binance Spot orders after user stream recovery")?;
    let mut emitted = AHashSet::new();
    let mut instruments = AHashSet::new();

    for report in &mut reports {
        normalize_recovered_order_status(report, ctx.treat_expired_as_canceled);
        emitted.insert(report.venue_order_id);
        instruments.insert(report.instrument_id);
    }

    for report in reports {
        ctx.emitter.send_order_status_report(report);
    }

    for identity in &ctx.dispatch_state.order_identities {
        instruments.insert(identity.instrument_id);
    }

    let now = ctx.clock.get_time_ns();
    let start = now.saturating_sub(DurationNanos::from_mins(ORDER_REPLAY_LOOKBACK_MINS));

    for instrument_id in instruments {
        if ctx.instrument_provider.excludes(instrument_id) {
            continue;
        }

        match ctx
            .http_client
            .request_order_history(ctx.account_id, instrument_id, Some(start), now)
            .await
        {
            Ok(history) => {
                for mut report in history {
                    if !emitted.insert(report.venue_order_id) {
                        continue;
                    }
                    normalize_recovered_order_status(&mut report, ctx.treat_expired_as_canceled);
                    ctx.emitter.send_order_status_report(report);
                }
            }
            Err(e) => {
                log::warn!(
                    "Binance Spot order history replay failed for {instrument_id} after user stream recovery: {e}"
                );
            }
        }
    }
    Ok(())
}

fn normalize_recovered_order_status(
    report: &mut OrderStatusReport,
    treat_expired_as_canceled: bool,
) {
    if treat_expired_as_canceled && report.order_status == OrderStatus::Expired {
        report.order_status = OrderStatus::Canceled;
    }
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;

    #[rstest]
    #[case::listen_key(
        BinanceSpotWsTradingMessage::ListenKeyExpired { event_time: 5 },
        SpotUserStreamRecoveryKind::ListenKeyExpired
    )]
    #[case::event_stream(
        BinanceSpotWsTradingMessage::EventStreamTerminated { event_time: 7 },
        SpotUserStreamRecoveryKind::EventStreamTerminated
    )]
    fn test_signal_user_stream_recovery_queues_expiry(
        #[case] message: BinanceSpotWsTradingMessage,
        #[case] expected: SpotUserStreamRecoveryKind,
    ) {
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        assert!(signal_user_stream_recovery(&message, &tx));
        assert_eq!(rx.try_recv().unwrap(), expected);
    }

    #[rstest]
    fn test_server_shutdown_does_not_request_recovery() {
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        let message = BinanceSpotWsTradingMessage::ServerShutdown { event_time: 1 };
        assert!(!signal_user_stream_recovery(&message, &tx));
        assert!(rx.try_recv().is_err());
    }

    #[rstest]
    fn test_signal_reports_when_recovery_task_is_gone() {
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        drop(rx);
        let message = BinanceSpotWsTradingMessage::ListenKeyExpired { event_time: 1 };
        assert!(!signal_user_stream_recovery(&message, &tx));
    }
}
