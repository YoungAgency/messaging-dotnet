using System;
using System.Threading;

namespace YoungMessaging.Abstractions{
    public interface IBusConsumer{
        void Subscribe<T,TH>(Func<TH> handler)
            where T : Event
            where TH : IEventHandler<T>;
    }
}