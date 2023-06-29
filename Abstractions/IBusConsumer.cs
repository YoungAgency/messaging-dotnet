using System;
using System.Threading.Tasks;

namespace YoungMessaging.Abstractions
{
    public interface IBusConsumer
    {
        Task Subscribe<T, TH>(IEventHandler<T> handler, string topicName, int maxConcurrent = 0)
            where T : Event
            where TH : IEventHandler<T>;

        Task SubscribeArray<T, TH>(IArrayEventHandler<T> handler, string topicName, int maxConcurrent = 0)
            where T : Event
            where TH : IArrayEventHandler<T>;
    }
}