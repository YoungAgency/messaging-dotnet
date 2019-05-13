using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace YoungMessaging.Abstractions{
        public interface IArrayEventHandler<in TEvent> : IArrayEventHandler
        where TEvent : Event
        {
            Task<EventResult> Handle(TEvent[] @event, CancellationToken token);
        }

        public interface IArrayEventHandler
        {
        }
    }