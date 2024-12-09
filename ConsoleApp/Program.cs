using System.Data;
using Microsoft.Data.SqlClient;
using Azure.Messaging.ServiceBus;
using System.Threading.Tasks;

namespace ConsoleApp
{
    public class Program
    {
        private static string topicName = "datachanges";
        private static string subscriptionName = "UserA_Subscription";
        private static string serviceBusConnectionString = "";
        private static string connectionString = "";

        static async Task SendMessageAsync(string messageBody)
        {
            await using var client = new ServiceBusClient(serviceBusConnectionString);
            ServiceBusSender sender = client.CreateSender(topicName);

            ServiceBusMessage message = new ServiceBusMessage(messageBody)
            {
                SessionId = "123",
                Subject = "Database Change Notification",
                ApplicationProperties = { { "Action", "Insert" } }
            };

            await sender.SendMessageAsync(message);
            Console.WriteLine("Message sent to Service Bus.");
        }

        static async Task ReceiveMessagesAsync()
        {
            await using var client = new ServiceBusClient(serviceBusConnectionString);
            ServiceBusSessionProcessor processor = client.CreateSessionProcessor(topicName, subscriptionName, new ServiceBusSessionProcessorOptions());

            processor.ProcessMessageAsync += MessageHandler;
            processor.ProcessErrorAsync += ErrorHandler;

            await processor.StartProcessingAsync();

            Console.WriteLine("Press [Enter] to stop the processor.");
            Console.ReadLine();

            await processor.StopProcessingAsync();
            Console.WriteLine("Processor stopped.");
        }

        static async Task MessageHandler(ProcessSessionMessageEventArgs args)
        {
            string messageBody = args.Message.Body.ToString();
            Console.WriteLine($"Received message: {messageBody}");

            // Tải dữ liệu thay đổi từ Azure SQL Server hoặc xử lý thông báo tại đây.
            await args.CompleteMessageAsync(args.Message);
        }

        static Task ErrorHandler(ProcessErrorEventArgs args)
        {
            Console.WriteLine($"Error: {args.Exception}");
            return Task.CompletedTask;
        }

        static void AddNewRecord()
        {
            using (SqlConnection conn = new SqlConnection(connectionString))
            {
                conn.Open();
                string query = "INSERT INTO Material (MaterialID, MaterialName, MaterialType, Unit) VALUES ('A2225', N'mút cắm hoa', N'phụ liệu', N'miếng')";
                using (SqlCommand cmd = new SqlCommand(query, conn))
                {
                    cmd.ExecuteNonQuery();
                    Console.WriteLine("New record added.");
                }
            }
        }
        public static async Task Main(string[] args)
        {

            // Khởi động lắng nghe thông báo
            _ = Task.Run(async () => await ReceiveMessagesAsync());

            Console.WriteLine("Listening for messages...");
            AddNewRecord();
            await SendMessageAsync("New record added to the database.");

            Console.ReadKey();
        }
    }
}
