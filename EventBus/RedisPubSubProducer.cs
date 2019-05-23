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
    public class RedisPubSubProducer : IBusProducer
    {
        private readonly BusSettings _busSettings;
        private IConnectionMultiplexer _conn;
        public RedisPubSubProducer(BusSettings busSettings)
        {
            _busSettings = busSettings;
            _conn = ConnectionMultiplexer.Connect(_busSettings.BusHost+":"+_busSettings.BusPort);
        }

        public async Task<bool> PublishAsync(Event message, string topicName)
        {
            if(!_conn.IsConnected) {
                _conn = ConnectionMultiplexer.Connect(_conn.Configuration);
            }
            var publisher = _conn.GetSubscriber();
            await publisher.PublishAsync(topicName, JsonConvert.SerializeObject(message));
            return true;
        }

        public async Task<bool> PublishAsync(Event[] messages, string topicName)
        {
            if(!_conn.IsConnected) {
                _conn = ConnectionMultiplexer.Connect(_conn.Configuration);
            }
            var publisher = _conn.GetSubscriber();
            await publisher.PublishAsync(topicName, JsonConvert.SerializeObject(messages));
            return true;
        }
    }
}