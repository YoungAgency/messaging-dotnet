using System.Threading;
using System.Threading.Tasks;

namespace YoungMessaging.Abstractions{
        public interface IEventHandler<in TEvent> : IEventHandler
        where TEvent : Event
        {
            Task<EventResult> Handle(TEvent @event, CancellationToken token);
        }

        public interface IEventHandler
        {
        }
    }