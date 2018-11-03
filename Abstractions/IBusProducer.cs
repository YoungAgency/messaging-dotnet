using System.Threading.Tasks;

namespace YoungMessaging.Abstractions{ 
    public interface IBusProducer{
        Task<bool> PublishAsync(Event message, string topicName);
    }
}