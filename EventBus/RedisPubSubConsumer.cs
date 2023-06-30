using System;
using System.Threading.Tasks;
using Google.Cloud.PubSub.V1;
using Grpc.Core;
using Newtonsoft.Json;
using System.Threading;
using Google.Api.Gax.Grpc;
using System.Collections.Generic;
using Google.Protobuf.Collections;
using YoungMessaging.Abstractions;
using YoungMessaging.Settings;
using Google.Protobuf;
using System.Text;
using StackExchange.Redis;

namespace YoungMessaging.EventBus
{
    public class RedisPubSubConsumer : IBusConsumer
    {
        private readonly BusSettings _busSettings;
        private IConnectionMultiplexer _conn;
        public RedisPubSubConsumer(BusSettings busSettings)
        {
            _busSettings = busSettings;
            _conn = ConnectionMultiplexer.Connect(_busSettings.BusHost + ":" + _busSettings.BusPort);
        }

        public void Subscribe<T, TH>(IEventHandler<T> handler, string topicName, int maxConcurrent = 1)
            where T : Event
            where TH : IEventHandler<T>
        {
            if (!_conn.IsConnected)
            {
                _conn = ConnectionMultiplexer.Connect(_conn.Configuration);
            }
            _conn.PreserveAsyncOrder = true;
            var subscriber = _conn.GetSubscriber();
            subscriber.Subscribe(new RedisChannel(topicName, RedisChannel.PatternMode.Auto), (channel, message) =>
            {
                T eventMessage = null;
                try
                {
                    eventMessage = JsonConvert.DeserializeObject<T>(message);
                }
                catch (JsonException ex)
                {
                    Console.WriteLine(ex.Message);
                    return;
                }

                handler.Handle(eventMessage, new CancellationToken());
            });
        }

        public void SubscribeArray<T, TH>(IArrayEventHandler<T> handler, string topicName, int maxConcurrent = 1)
            where T : Event
            where TH : IArrayEventHandler<T>
        {
            if (!_conn.IsConnected)
            {
                _conn = ConnectionMultiplexer.Connect(_conn.Configuration);
            }
            var subscriber = _conn.GetSubscriber();
            subscriber.Subscribe(new RedisChannel(topicName, RedisChannel.PatternMode.Auto), (channel, message) =>
            {
                T[] events = null;
                try
                {
                    events = JsonConvert.DeserializeObject<T[]>(message);
                }
                catch (JsonException ex)
                {
                    Console.WriteLine(ex.Message);
                }

                var result = handler.Handle(events, new CancellationToken());
            });
        }
    }
}