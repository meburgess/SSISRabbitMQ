using Microsoft.SqlServer.Dts.Pipeline;
using Microsoft.SqlServer.Dts.Pipeline.Wrapper;
using Microsoft.SqlServer.Dts.Runtime;
using Microsoft.SqlServer.Dts.Runtime.Wrapper;
using RabbitMQ.Client;
using System;
using System.Text;

// https://kzhendev.wordpress.com/2013/08/13/part-3-building-a-custom-source-component/
namespace SSISRabbitMQ.RabbitMQSource
{
    [DtsPipelineComponent(IconResource = "SSISRabbitMQ.RabbitMQSource.Rabbit.ico",
        DisplayName = "RabbitMQ Source",
        ComponentType = ComponentType.SourceAdapter,
        Description = "Connection source for RabbitMQ",
        UITypeName = "SSISRabbitMQ.RabbitMQSource.RabbitMQSourceUI, SSISRabbitMQ.RabbitMQSource, Version=1.0.0.0, Culture=neutral, PublicKeyToken=ac1c316408dd3955")]
    public class RabbitMQSource : PipelineComponent
    {
        private IConnection rabbitConnection;
        private IModel consumerChannel;

        private RabbitMQConnectionManager.RabbitMQConnectionManager rabbitMqConnectionManager;

        private string queueName;

        public override void ProvideComponentProperties()
        {
            // Reset the component.
            base.RemoveAllInputsOutputsAndCustomProperties();
            ComponentMetaData.RuntimeConnectionCollection.RemoveAll();

            var output = ComponentMetaData.OutputCollection.New();
            output.Name = "Output";

            var queueName = ComponentMetaData.CustomPropertyCollection.New();
            queueName.Name = "QueueName";
            queueName.Description = "The name of the RabbitMQ queue to read messages from";

            var connection = ComponentMetaData.RuntimeConnectionCollection.New();
            connection.Name = "RabbitMQ";
            connection.ConnectionManagerID = "RabbitMQ";

            CreateColumns();
        }

        private void CreateColumns()
        {
            var output = ComponentMetaData.OutputCollection[0];

            output.OutputColumnCollection.RemoveAll();
            output.ExternalMetadataColumnCollection.RemoveAll();

            var column1 = output.OutputColumnCollection.New();
            var exColumn1 = output.ExternalMetadataColumnCollection.New();

            var column2 = output.OutputColumnCollection.New();
            var exColumn2 = output.ExternalMetadataColumnCollection.New();

            column1.Name = "MessageContents";
            column1.SetDataTypeProperties(DataType.DT_WSTR, 4000, 0, 0, 0);

            column2.Name = "RoutingKey";
            column2.SetDataTypeProperties(DataType.DT_WSTR, 100, 0, 0, 0);
        }

        public override DTSValidationStatus Validate()
        {
            string qName = ComponentMetaData.CustomPropertyCollection["QueueName"].Value;

            if (string.IsNullOrWhiteSpace(qName))
            {
                //Validate that the QueueName property is set
                ComponentMetaData.FireError(0, ComponentMetaData.Name, "The QueueName property must be set", "", 0, out bool cancel);
                return DTSValidationStatus.VS_ISBROKEN;
            }

            return base.Validate();
        }

        public override void AcquireConnections(object transaction)
        {
            if (ComponentMetaData.RuntimeConnectionCollection[0].ConnectionManager != null)
            {
                ConnectionManager connectionManager = DtsConvert.GetWrapper(
                  ComponentMetaData.RuntimeConnectionCollection[0].ConnectionManager);

                this.rabbitMqConnectionManager = connectionManager.InnerObject as RabbitMQConnectionManager.RabbitMQConnectionManager;

                if (this.rabbitMqConnectionManager == null)
                    throw new Exception("Couldn't get the RabbitMQ connection manager, ");

                this.queueName = ComponentMetaData.CustomPropertyCollection["QueueName"].Value;

                rabbitConnection = this.rabbitMqConnectionManager.AcquireConnection(transaction) as IConnection;
            }
        }

        public override void ReleaseConnections()
        {
            if (rabbitMqConnectionManager != null)
            {
                this.rabbitMqConnectionManager.ReleaseConnection(rabbitConnection);
            }
        }

        public override void PreExecute()
        {
            try
            {
                this.consumerChannel = rabbitConnection.CreateModel();
                this.consumerChannel.QueueDeclare(queueName, true, false, false, null);
            }
            catch (Exception)
            {
                ReleaseConnections();
                throw;
            }
        }

        public override void PrimeOutput(int outputs, int[] outputIDs, PipelineBuffer[] buffers)
        {
            IDTSOutput100 output = ComponentMetaData.OutputCollection[0];
            PipelineBuffer buffer = buffers[0];

            var result = consumerChannel.BasicGet(queueName, true);
            while (result != null)
            {
                var messageContent = Encoding.UTF8.GetString(result.Body);

                buffer.AddRow();
                buffer[0] = messageContent;
                buffer[1] = result.RoutingKey;

                result = consumerChannel.BasicGet(queueName, true);
            }

            buffer.SetEndOfRowset();
        }

        public override void Cleanup()
        {
            if (consumerChannel.IsOpen)
            {
                consumerChannel.Close();
            }
            base.Cleanup();
        }
    }
}
