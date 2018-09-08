using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using RabbitMQ.Client.Framing.Impl;
using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Backend
{
    class Program
    {
        private const char KEY_AND_VALUE_SEPARATOR = '=';

        private const char PARAMETERS_SEPARATOR = ';';
        private const string PARAMETER_USER = "user";
        private const string PARAMETER_PASSWORD = "password";
        private const string PARAMETER_QUANITY = "quanity";
        private const string PARAMETER_INSTALLMENT = "installment";
        private const string PARAMETER_INTEREST = "interest";

        private const char OPERATION_SEPARATOR = '-';
        private const string OPERATION_LOGIN = "login";
        private const string OPERATION_LOAN = "loan";

        private const int MAX_LOAN = 200000;
        private const float EXPECTED_MONTHLY_INTEREST = 0.01f;

        private static string GetOperation(string message)
        {
            return message.Split(OPERATION_SEPARATOR)[0];
        }

        private static string[] GetParameters(string message)
        {
            return message.Split(OPERATION_SEPARATOR)[1].Split(PARAMETERS_SEPARATOR);
        }

        private static Tuple<string, string> GetKeyAndValue(string separatedString)
        {
            var keyAndValue = separatedString.Split(KEY_AND_VALUE_SEPARATOR);
            return Tuple.Create(keyAndValue[0], keyAndValue[1]);
        }

        private static void Receive(object model, BasicDeliverEventArgs ea)
        {
            var body = ea.Body;
            var message = Encoding.UTF8.GetString(body);
            var operation = GetOperation(message);
            var parameters = GetParameters(message);

            string user = null;
            string password = null;
            float quanity = 0;
            float installment = 0;
            float interest = 0;

            foreach (string parameter in parameters)
            {
                var keyAndValue = GetKeyAndValue(parameter);
                var key = keyAndValue.Item1;
                var value = keyAndValue.Item2;
                switch (key)
                {
                    case PARAMETER_USER:
                        user = value;
                        break;
                    case PARAMETER_PASSWORD:
                        password = value;
                        break;
                    case PARAMETER_QUANITY:
                        quanity = float.Parse(value);
                        break;
                    case PARAMETER_INSTALLMENT:
                        installment = float.Parse(value);
                        break;
                    case PARAMETER_INTEREST:
                        interest = float.Parse(value);
                        break;
                    default:
                        break;
                }
            }
            string returnString;
            switch (operation)
            {
                case OPERATION_LOAN:
                    returnString = LoanService(user, quanity, installment, interest);
                    break;
                case OPERATION_LOGIN:
                    returnString = LoginService(user, password);
                    break;
                default:
                    returnString = "404";
                    break;
            }
            var responseBytes = Encoding.UTF8.GetBytes(returnString);
            var replyProps = ((Model)model).CreateBasicProperties();
            replyProps.CorrelationId = ea.BasicProperties.CorrelationId;
            ((Model)model).BasicPublish("", ea.BasicProperties.ReplyTo, replyProps, responseBytes);
        }

        private static string LoginService(string user, string password)
        {
            var sqlConnection = new SqlConnection
            {
                ConnectionString = "Data Source=(localdb)\\MSSQLLocalDB;" +
                "Initial Catalog=loans;" + "" +
                "Integrated Security=True;" +
                "Connect Timeout=30;" +
                "Encrypt=False;" +
                "TrustServerCertificate=False;" +
                "ApplicationIntent=ReadWrite;" +
                "MultiSubnetFailover=False;"
            };
            sqlConnection.Open();
            var cmd = sqlConnection.CreateCommand();
            cmd.CommandText = String.Format("select password from [dbo.Users] where login = {0}", user);
            string gottenPassword = (string)cmd.ExecuteScalar();
            if (gottenPassword != password)
            {
                return "402";
            }
            cmd.CommandText = String.Format("select number, account, name, surname from [dbo.Users] where login = {0};", user);
            var executeReader = cmd.ExecuteReader();
            string number = executeReader.GetString(0);
            float account = executeReader.GetFloat(1);
            string name = executeReader.GetString(2);
            string surname = executeReader.GetString(3);
            cmd.CommandText = String.Format("update [dbo.Users] set account={0} where login={1};", account, user);
            cmd.ExecuteNonQuery();
            sqlConnection.Close();
            return String.Format("201-{0};{1};{2};{3}", number, account, name, surname);
        }

        private static string LoanService(string user, float quanity, float installment, float interest)
        {
            if (quanity != 0 && installment != 0 && interest != 0 && !(user is null))
            {
                float expected_interest = EXPECTED_MONTHLY_INTEREST * (1 + quanity / MAX_LOAN);
                if (interest / installment >= expected_interest)
                {
                    return TakeLoan(user, quanity, installment, interest);
                }
            }
            return "402";
        }

        private static string TakeLoan(string user, float quanity, float installment, float interest)
        {
            var sqlConnection = new SqlConnection
            {
                ConnectionString = "Data Source=(localdb)\\MSSQLLocalDB;" +
                "Initial Catalog=loans;" + "" +
                "Integrated Security=True;" +
                "Connect Timeout=30;" +
                "Encrypt=False;" +
                "TrustServerCertificate=False;" +
                "ApplicationIntent=ReadWrite;" +
                "MultiSubnetFailover=False;"
            };
            sqlConnection.Open();
            var cmd = sqlConnection.CreateCommand();
            cmd.CommandText = String.Format("insert into [dbo.loans] (user, quanity, interest, installment);" +
                " values({0}, {1}, {2}, {3})", user, quanity, interest, installment);
            cmd.ExecuteNonQuery();
            cmd.CommandText = String.Format("select number, account, name, surname from [dbo.Users] where login = {0};", user);
            var executeReader = cmd.ExecuteReader();
            string number = executeReader.GetString(0);
            float account = executeReader.GetFloat(1);
            string name = executeReader.GetString(2);
            string surname = executeReader.GetString(3);
            account += quanity;
            cmd.CommandText = String.Format("update [dbo.Users] set account={0} where login={1};", account, user);
            cmd.ExecuteNonQuery();
            sqlConnection.Close();
            return String.Format("201-{0};{1};{2};{3}", number, account, name, surname);
        }

        static void Main(string[] args)
        {
            var factory = new ConnectionFactory()
            {
                UserName = "guest",
                Password = "guest",
                HostName = "localhost",
                VirtualHost = "103057"
            };
            using (var connection = factory.CreateConnection())
            using (var channel = connection.CreateModel())
            {
                var queue = channel.QueueDeclare("loan_queue", false, false, false, null);
                var consumer = new EventingBasicConsumer(channel);
                consumer.Received += Receive;

                while (true)
                {
                    channel.BasicConsume("rpc_queue", true, consumer);
                }
            }
        }
    }
}
