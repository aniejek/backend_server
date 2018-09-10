﻿using RabbitMQ.Client;
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
        private const int PARAMETER_LOGIN = 0;
        private const int PARAMETER_PASSWORD = 1;
        private const int PARAMETER_QUANITY = 0;
        private const int PARAMETER_INTEREST = 1;
        private const int PARAMETER_INSTALLMENT = 2;
        private const int PARAMETER_ACCOUNT_NUMBER = 3;

        private const char OPERATION_SEPARATOR = '-';
        private const string OPERATION_LOGIN = "logowanie";
        private const string OPERATION_LOAN = "pozyczka";

        private const int MAX_LOAN = 200000;
        private const double EXPECTED_MONTHLY_INTEREST = 0.01f;

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
            Console.WriteLine(String.Format("Otrzymano wiadomosc: {0}", message));

            string returnString;
            switch (operation)
            {
                case OPERATION_LOAN:
                    double quanity = double.Parse(parameters[PARAMETER_QUANITY]);
                    double interest = double.Parse(parameters[PARAMETER_INTEREST]);
                    double installment = double.Parse(parameters[PARAMETER_INSTALLMENT]);
                    string accountNumber = parameters[PARAMETER_ACCOUNT_NUMBER];
                    returnString = LoanService(quanity, interest, installment, accountNumber);
                    break;
                case OPERATION_LOGIN:
                    string login = parameters[PARAMETER_LOGIN];
                    string password = parameters[PARAMETER_PASSWORD];
                    returnString = LoginService(login, password);
                    break;
                default:
                    returnString = "404";
                    break;
            }

            Console.WriteLine(String.Format("Zwracam wiadomosc: {0}", returnString));

            //var responseBytes = Encoding.UTF8.GetBytes(returnString);
            //var replyProps = model.CreateBasicProperties();
            //replyProps.CorrelationId = ea.BasicProperties.CorrelationId;
            //((Model)model).BasicPublish("", ea.BasicProperties.ReplyTo, replyProps, responseBytes);
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
            cmd.CommandText = String.Format("select password from users where login = '{0}'", user);
            string gottenPassword = (string)cmd.ExecuteScalar();
            if (gottenPassword != password)
            {
                return "402";
            }
            cmd.CommandText = String.Format("select number, account, name, surname from users where login = '{0}';", user);
            var executeReader = cmd.ExecuteReader();
            executeReader.Read();
            string number = executeReader.GetString(0);
            double account = executeReader.GetFloat(1);
            string name = executeReader.GetString(2);
            string surname = executeReader.GetString(3);
            executeReader.Close();
            cmd.CommandText = String.Format("update users set account='{0}' where login='{1}';", account, user);
            cmd.ExecuteNonQuery();
            sqlConnection.Close();
            return String.Format("201-{0};{1};{2};{3}", number, account, name, surname);
        }

        private static string LoanService(double quanity, double interest, double installment, string accountNumber)
        {
            if (quanity != 0 && installment != 0 && interest != 0 && !(accountNumber is null))
            {
                double expected_interest = EXPECTED_MONTHLY_INTEREST * (1 + quanity / MAX_LOAN);
                if (interest / installment >= expected_interest)
                {
                    return TakeLoan(accountNumber, quanity, installment, interest);
                }
            }
            return "402";
        }

        private static string TakeLoan(string accountNumber, double quanity, double installment, double interest)
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
            cmd.CommandText = String.Format("select login from users where number = '{0}'", accountNumber);
            string login = (string)cmd.ExecuteScalar();

            cmd.CommandText = String.Format("insert into loans (login, quanity, interest, installment) values('{0}', '{1}', '{2}', '{3}');",
                login, quanity, interest, installment);
            cmd.ExecuteNonQuery();

            cmd.CommandText = String.Format("select account, name, surname from users where login = '{0}';", login);
            var executeReader = cmd.ExecuteReader();
            executeReader.Read();
            double account = (double)executeReader.GetDouble(0);
            string name = executeReader.GetString(1);
            string surname = executeReader.GetString(2);
            executeReader.Close();

            account += quanity;
            cmd.CommandText = String.Format("update users set account='{0}' where login='{1}';", account, login);
            cmd.ExecuteNonQuery();
            sqlConnection.Close();
            return String.Format("201-{0};{1};{2};{3}", accountNumber, account, name, surname);
        }

        static void Main(string[] args)
        {
            Console.WriteLine("Start serwera backendowego. Aby zatrzymać wciśnij Escape.");
            var factory = new ConnectionFactory()
            {
                UserName = "guest",
                Password = "guest",
                HostName = "localhost",
                VirtualHost = "/"
            };
            Console.WriteLine("Przygotowywanie kanału");
            using (var connection = factory.CreateConnection())
            using (var channel = connection.CreateModel())
            {
                channel.ContinuationTimeout = new TimeSpan(0, 2, 0);
                Console.WriteLine("Przygotowywanie kolejki.");
                var queue = channel.QueueDeclare("loan_queue", false, false, false, null);
                Console.WriteLine("Przygotowywanie konsumenta.");
                var consumer = new EventingBasicConsumer(channel);
                //consumer.Received += Receive;
                consumer.Received += (model, ea) =>
                {
                    var body = ea.Body;
                    var message = Encoding.UTF8.GetString(body);
                    var operation = GetOperation(message);
                    var parameters = GetParameters(message);
                    Console.WriteLine(String.Format("Otrzymano wiadomosc: {0}", message));

                    string returnString;
                    switch (operation)
                    {
                        case OPERATION_LOAN:
                            double quanity = double.Parse(parameters[PARAMETER_QUANITY]);
                            double interest = double.Parse(parameters[PARAMETER_INTEREST]);
                            double installment = double.Parse(parameters[PARAMETER_INSTALLMENT]);
                            string accountNumber = parameters[PARAMETER_ACCOUNT_NUMBER];
                            returnString = LoanService(quanity, interest, installment, accountNumber);
                            break;
                        case OPERATION_LOGIN:
                            string login = parameters[PARAMETER_LOGIN];
                            string password = parameters[PARAMETER_PASSWORD];
                            returnString = LoginService(login, password);
                            break;
                        default:
                            returnString = "404";
                            break;
                    }

                    Console.WriteLine(String.Format("Zwracam wiadomosc: {0}", returnString));

                    var responseBytes = Encoding.UTF8.GetBytes(returnString);
                    var replyProps = channel.CreateBasicProperties();
                    replyProps.CorrelationId = ea.BasicProperties.CorrelationId;
                    channel.BasicPublish("", (string)ea.BasicProperties.ReplyTo, null, responseBytes);
                };
                Console.WriteLine("Rozpoczynam konsumowanie.");
                do
                {
                    Console.WriteLine("Aby zakończyć, kliknij escape.");
                    while (!Console.KeyAvailable)
                    {
                        channel.BasicConsume("loan_queue", false, consumer);
                    }
                } while (Console.ReadKey(true).Key != ConsoleKey.Escape);
            }
        }
    }
}
