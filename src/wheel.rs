pub mod core;
pub mod interpret;
pub mod performer_sklave;

mod lru;

// async fn busyloop<P>(
//     mut supervisor_pid: SupervisorPid,
//     meister: performer_sklave::Meister,
//     mut fused_performer_sklave_error_rx: stream::Fuse<mpsc::UnboundedReceiver<performer_sklave::Error>>,
//     mut state: State<P>,
// )
//     -> Result<(), ErrorSeverity<State<P>, Error>>
// where P: edeltraud::ThreadPool<job::Job> + Clone + Send + 'static,
// {
//     enum Mode {
//         Regular,
//         AwaitFlush {
//             done_rx: oneshot::Receiver<Flushed>,
//             reply_tx: oneshot::Sender<Flushed>,
//         },
//     }

//     let mut mode = Mode::Regular;

//     log::debug!("starting wheel busyloop");
//     loop {
//         enum Event<R, E, F> {
//             Request(Option<R>),
//             PerformerError(E),
//             FlushDone { result: F, reply_tx: oneshot::Sender<Flushed>, },
//         }

//         let event = match mode {
//             Mode::Regular => {
//                 select! {
//                     result = state.fused_request_rx.next() =>
//                         Event::Request(result),
//                     result = fused_performer_sklave_error_rx.next() =>
//                         Event::PerformerError(result),
//                 }
//             },
//             Mode::AwaitFlush { mut done_rx, reply_tx, } =>
//                 select! {
//                     result = fused_performer_sklave_error_rx.next() => {
//                         Event::PerformerError(result)
//                     },
//                     result = &mut done_rx => {
//                         Event::FlushDone { result, reply_tx, }
//                     },
//                 },
//         };
//         mode = Mode::Regular;

//         match event {

//             Event::Request(None) => {
//                 log::info!("requests sink channel depleted: terminating");
//                 return Ok(());
//             },

//             Event::Request(Some(proto::Request::Flush(proto::RequestFlush { context: reply_tx, }))) => {
//                 let (done_tx, done_rx) = oneshot::channel();
//                 mode = Mode::AwaitFlush { done_rx, reply_tx, };
//                 let order = performer_sklave::Order::Request(
//                     proto::Request::Flush(proto::RequestFlush { context: done_tx, }),
//                 );
//                 match meister.befehl(order, &state.thread_pool) {
//                     Ok(()) =>
//                         (),
//                     Err(arbeitssklave::Error::Terminated) => {
//                         log::debug!("performer sklave is gone, terminating");
//                         return Ok(());
//                     },
//                     Err(error) =>
//                         return Err(ErrorSeverity::Fatal(Error::Arbeitssklave(error))),
//                 }
//             },

//             Event::Request(Some(proto::Request::IterBlocks(proto::RequestIterBlocks {
//                 context: IterBlocksContext {
//                     reply_tx,
//                     maybe_iter_task_tx: None,
//                 },
//             }))) => {
//                 let (iter_task_tx, iter_task_rx) = oneshot::channel();
//                 supervisor_pid.spawn_link_temporary(
//                     forward_blocks_iter(
//                         iter_task_rx,
//                         meister.clone(),
//                         state.thread_pool.clone(),
//                     ),
//                 );
//                 let order = performer_sklave::Order::Request(
//                     proto::Request::IterBlocks(proto::RequestIterBlocks {
//                         context: IterBlocksContext {
//                             reply_tx,
//                             maybe_iter_task_tx: Some(iter_task_tx),
//                         },
//                     }),
//                 );
//                 match meister.befehl(order, &state.thread_pool) {
//                     Ok(()) =>
//                         (),
//                     Err(arbeitssklave::Error::Terminated) => {
//                         log::debug!("performer sklave is gone, terminating");
//                         return Ok(());
//                     },
//                     Err(error) =>
//                         return Err(ErrorSeverity::Fatal(Error::Arbeitssklave(error))),
//                 }
//             },

//             Event::Request(Some(proto::Request::IterBlocks(proto::RequestIterBlocks {
//                 context: IterBlocksContext {
//                     maybe_iter_task_tx: Some(..),
//                     ..
//                 },
//             }))) =>
//                 unreachable!(),

//             Event::Request(Some(request)) => {
//                 let order = performer_sklave::Order::Request(request);
//                 match meister.befehl(order, &state.thread_pool) {
//                     Ok(()) =>
//                         (),
//                     Err(arbeitssklave::Error::Terminated) => {
//                         log::debug!("performer sklave is gone, terminating");
//                         return Ok(());
//                     },
//                     Err(error) =>
//                         return Err(ErrorSeverity::Fatal(Error::Arbeitssklave(error))),
//                 }
//             },

//             Event::PerformerError(Some(error)) =>
//                 return Err(ErrorSeverity::Fatal(Error::PerformerSklave(error))),

//             Event::PerformerError(None) => {
//                 log::debug!("interpreter error channel closed: shutting down");
//                 return Ok(());
//             },

//             Event::FlushDone { result: Ok(Flushed), reply_tx, } =>
//                 if let Err(_send_error) = reply_tx.send(Flushed) {
//                     log::warn!("Pid is gone during Flush query result send");
//                 },

//             Event::FlushDone { result: Err(oneshot::Canceled), .. } => {
//                 log::debug!("flushed notify channel closed: shutting down");
//                 return Ok(());
//             },

//         }
//     }
// }

// pub enum IterTask {
//     Taken,
//     Item {
//         blocks_tx: mpsc::Sender<IterBlocksItem>,
//         item: IterBlocksItem,
//         iter_blocks_cursor: performer::IterBlocksCursor,
//     },
//     Finish {
//         blocks_tx: mpsc::Sender<IterBlocksItem>,
//     },
// }

// pub enum IterTaskDone {
//     PeerLost,
//     ItemSent {
//         iter_blocks_state: performer::IterBlocksState<<Context as context::Context>::IterBlocksStream>,
//         iter_task_rx: oneshot::Receiver<IterTask>,
//     },
//     Finished,
// }

// use std::{
//     mem,
//     pin::Pin,
//     task::Poll,
// };

// impl future::Future for IterTask {
//     type Output = IterTaskDone;

//     fn poll(self: Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> Poll<Self::Output> {
//         let this = self.get_mut();
//         match mem::replace(this, IterTask::Taken) {
//             IterTask::Taken =>
//                 panic!("polled IterTask after completion"),
//             IterTask::Item { mut blocks_tx, item, iter_blocks_cursor, } => {
//                 let mut sink = Pin::new(&mut blocks_tx);
//                 match sink.as_mut().poll_ready(cx) {
//                     Poll::Ready(Ok(())) =>
//                         (),
//                     Poll::Ready(Err(_send_error)) =>
//                         return Poll::Ready(IterTaskDone::PeerLost),
//                     Poll::Pending => {
//                         *this = IterTask::Item { blocks_tx, item, iter_blocks_cursor, };
//                         return Poll::Pending;
//                     },
//                 }
//                 match sink.as_mut().start_send(item) {
//                     Ok(()) => {
//                         let (iter_task_tx, iter_task_rx) = oneshot::channel();
//                         Poll::Ready(IterTaskDone::ItemSent {
//                             iter_blocks_state: performer::IterBlocksState {
//                                 iter_blocks_stream_context: IterBlocksStreamContext {
//                                     blocks_tx,
//                                     iter_task_tx,
//                                 },
//                                 iter_blocks_cursor,
//                             },
//                             iter_task_rx,
//                         })
//                     },
//                     Err(_send_error) =>
//                         Poll::Ready(IterTaskDone::PeerLost),
//                 }
//             },
//             IterTask::Finish { mut blocks_tx, } => {
//                 let mut sink = Pin::new(&mut blocks_tx);
//                 match sink.as_mut().poll_ready(cx) {
//                     Poll::Ready(Ok(())) =>
//                         (),
//                     Poll::Ready(Err(_send_error)) =>
//                         return Poll::Ready(IterTaskDone::PeerLost),
//                     Poll::Pending => {
//                         *this = IterTask::Finish { blocks_tx, };
//                         return Poll::Pending;
//                     },
//                 }
//                 match sink.as_mut().start_send(IterBlocksItem::NoMoreBlocks) {
//                     Ok(()) =>
//                         Poll::Ready(IterTaskDone::Finished),
//                     Err(_send_error) =>
//                         Poll::Ready(IterTaskDone::PeerLost),
//                 }
//             },
//         }
//     }
// }

// async fn forward_blocks_iter<P>(
//     iter_task_rx: oneshot::Receiver<IterTask>,
//     meister: performer_sklave::Meister,
//     thread_pool: P,
// )
// where P: edeltraud::ThreadPool<job::Job>,
// {
//     if let Err(error) = run_forward_blocks_iter(iter_task_rx, meister, thread_pool).await {
//         log::debug!("canceling blocks iterator forwarding because of: {error:?}");
//     }
// }

// #[derive(Debug)]
// enum ForwardBlocksIterError {
//     PerformerSklaveIsLost,
//     StreamPeerLost,
//     Arbeitssklave(arbeitssklave::Error),
// }

// async fn run_forward_blocks_iter<P>(
//     mut iter_task_rx: oneshot::Receiver<IterTask>,
//     meister: performer_sklave::Meister,
//     thread_pool: P,
// )
//     -> Result<(), ForwardBlocksIterError>
// where P: edeltraud::ThreadPool<job::Job>,
// {
//     loop {
//         let iter_task = iter_task_rx.await
//             .map_err(|oneshot::Canceled| ForwardBlocksIterError::PerformerSklaveIsLost)?;

//         match iter_task.await {
//             IterTaskDone::PeerLost =>
//                 return Err(ForwardBlocksIterError::StreamPeerLost),
//             IterTaskDone::ItemSent { iter_blocks_state, iter_task_rx: next_iter_task_rx, } => {
//                 let order = performer_sklave::Order::IterBlocks(
//                     performer_sklave::OrderIterBlocks {
//                         iter_blocks_state,
//                     },
//                 );
//                 meister.befehl(order, &thread_pool)
//                     .map_err(ForwardBlocksIterError::Arbeitssklave)?;
//                 iter_task_rx = next_iter_task_rx;
//             },
//             IterTaskDone::Finished =>
//                 return Ok(()),
//         }
//     }
// }
