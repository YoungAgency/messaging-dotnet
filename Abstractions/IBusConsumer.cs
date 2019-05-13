using System;
using System.Threading;
using System.Threading.Tasks;

namespace YoungMessaging.Abstractions{
    public interface IBusConsumer{
        void Subscribe<T,TH>(Func<TH> handler, string topicName)
            where T : Event
            where TH : IEventHandler<T>;

        void SubscribeArray<T,TH>(Func<TH> handler, string topicName)
            where T : Event
            where TH : IArrayEventHandler<T>;
    }
}