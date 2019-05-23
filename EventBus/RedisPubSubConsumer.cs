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
            _conn = ConnectionMultiplexer.Connect(_busSettings.BusHost+":"+_busSettings.BusPort);
        }

        public void Subscribe<T, TH>(Func<TH> handler, string topicName)
            where T : Event
            where TH : IEventHandler<T>
        {
            if(!_conn.IsConnected) {
                _conn = ConnectionMultiplexer.Connect(_conn.Configuration);
            }
            var subscriber = _conn.GetSubscriber();
            subscriber.Subscribe(topicName, (channel, message)=> {
                T eventMessage = null;
                try{
                    eventMessage = JsonConvert.DeserializeObject<T>(message);
                }catch(JsonException ex){
                    Console.WriteLine(ex.Message);
                    return;
                }
                var invoke = handler.DynamicInvoke();
                var concreteType = typeof(IEventHandler<>).MakeGenericType(typeof(T));
                var task = (Task<EventResult>) concreteType.GetMethod("Handle").Invoke(invoke, new object[] { eventMessage, null });
                task.GetAwaiter();
            });
        }

        public void SubscribeArray<T, TH>(Func<TH> handler, string topicName)
            where T : Event
            where TH : IArrayEventHandler<T>
        {
            if(!_conn.IsConnected) {
                _conn = ConnectionMultiplexer.Connect(_conn.Configuration);
            }
            var subscriber = _conn.GetSubscriber();
            subscriber.Subscribe(topicName, (channel, message)=> {
                T[] events = null;
                try{
                    events = JsonConvert.DeserializeObject<T[]>(message);
                }catch(JsonException ex){
                    Console.WriteLine(ex.Message);
                }
                var invoke = handler.DynamicInvoke();
                var concreteType = typeof(IArrayEventHandler<>).MakeGenericType(typeof(T));
                var task = (Task<EventResult>) concreteType.GetMethod("Handle").Invoke(invoke, new object[] { events, null });
                task.GetAwaiter();
            });
        }
    }
}