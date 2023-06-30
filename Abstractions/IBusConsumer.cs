using System;
using System.Threading.Tasks;

namespace YoungMessaging.Abstractions
{
    public interface IBusConsumer
    {
        void Subscribe<T, TH>(IEventHandler<T> handler, string topicName, int maxConcurrent = 0)
            where T : Event
            where TH : IEventHandler<T>;

        void SubscribeArray<T, TH>(IArrayEventHandler<T> handler, string topicName, int maxConcurrent = 0)
            where T : Event
            where TH : IArrayEventHandler<T>;
    }
}