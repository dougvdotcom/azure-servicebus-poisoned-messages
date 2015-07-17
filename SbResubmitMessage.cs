using System;
using System.Net;
using System.Threading;
using Microsoft.ServiceBus.Messaging;
using Microsoft.WindowsAzure;
using Microsoft.WindowsAzure.ServiceRuntime;
using NLog;

namespace WorkerRoleWithSBQueue1
{
	public class WorkerRole : RoleEntryPoint
	{
		const string QueueName = "MY-QUEUE-NAME";

		QueueClient _client;
		readonly ManualResetEvent _completedEvent = new ManualResetEvent(false);

		// I like to use NLog for logging; it has native support for Trace logging to Azure Diagnostics
		private static readonly Logger Log = LogManager.GetCurrentClassLogger();

		public override void Run()
		{
			Log.Trace("Starting processing of messages");
			_client.OnMessage(receivedMessage =>
			{
				try
				{
					// message is OK if try succeeds
					Log.Info("Processing Service Bus message: {0}", receivedMessage.Label);
					// your processing code here
					Log.Info("Processing of {0} complete.", receivedMessage.Label);
				}
				catch(Exception ex)
				{
					// Handle any message processing specific exceptions here
					Log.Error("Could not process {0}. Reason: {1}", receivedMessage.Label, ex.Message);
					
					// recreate message and send it again
					var newMsg = new BrokeredMessage {
						Label = receivedMessage.Label;
					};
					_client.Send(newMsg);
				}
				finally {
					// complete the message, regardless of success or failure
					receivedMessage.Complete();
				}
			});

			_completedEvent.WaitOne();
		}

		public override bool OnStart()
		{
			// Set the maximum number of concurrent connections 
			ServicePointManager.DefaultConnectionLimit = 12;

			// Create the queue if it does not exist already
			var connectionString = CloudConfigurationManager.GetSetting("Microsoft.ServiceBus.ConnectionString");

			// Initialize the connection to Service Bus Queue
			_client = QueueClient.CreateFromConnectionString(connectionString, QueueName);
			return base.OnStart();
		}

		public override void OnStop()
		{
			// Close the connection to Service Bus Queue
			_client.Close();
			_completedEvent.Set();
			base.OnStop();
		}
	}
}