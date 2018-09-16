using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Mail;
using System.Net;
using System.Text;
using System.Threading.Tasks;
using System.Net.Sockets;

namespace MailServer
{
    class Program
    {
        private const char PARAMETERS_SEPARATOR = ';';

        private const int PARAMETER_LOGIN = 0;
        private const int PARAMETER_QUANITY = 1;
        private const int PARAMETER_INTEREST = 2;
        private const int PARAMETER_INSTALLMENT = 3;
        private const int PARAMETER_NAME = 4;
        private const int PARAMETER_SURNAME = 5;

        private static string[] GetParameters(string message)
        {
            return message.Split(PARAMETERS_SEPARATOR);
        }

        private const string MAIL_QUEUE = "mail_queue";
        static void Main(string[] args)
        {
            Console.WriteLine("Start serwera mailowego.");
            var client = new SmtpClient();
            client.DeliveryMethod = SmtpDeliveryMethod.Network;
            client.Host = "smtp.poczta.onet.pl";
            client.Port = 465;
            client.EnableSsl = true;
            client.Timeout = 100000;
            client.UseDefaultCredentials = false;
            client.Credentials = new NetworkCredential("aleksy.dobrodziejow@onet.pl", "******");
            Console.WriteLine("Przygotowywanie ConnectionFactory.");
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
                Console.WriteLine("Przygotowywanie kolejki mailowej.");
                var email_queue = channel.QueueDeclare(MAIL_QUEUE, false, false, false, null);
                Console.WriteLine("Przygotowywanie konsumenta.");
                var consumer = new EventingBasicConsumer(channel);
                //consumer.Received += Receive;
                consumer.Received += (model, ea) =>
                {
                    var body = ea.Body;
                    var message = Encoding.UTF8.GetString(body);
                    var parameters = GetParameters(message);
                    Console.WriteLine(String.Format("Otrzymano wiadomosc: {0}", message));

                    string email = parameters[PARAMETER_LOGIN];
                    string quanity = parameters[PARAMETER_QUANITY];
                    string interest = parameters[PARAMETER_INTEREST];
                    string installment = parameters[PARAMETER_INSTALLMENT];
                    string name = parameters[PARAMETER_NAME];
                    string surname = parameters[PARAMETER_SURNAME];

                    sendMail(client, name, surname, email, quanity, interest, installment);
                };
                channel.BasicConsume(MAIL_QUEUE, true, consumer);
                Console.WriteLine("Rozpoczynam konsumowanie.");
                while (Console.ReadKey().Key != ConsoleKey.Q) ;
            }
        }

        private static void sendMail(SmtpClient client, string name, string surname, string email, string quanity, string interest, string installment)
        {
            var mail = new MailMessage("aleksy.dobrodziejow@onet.pl", email)
            {
                Subject = "Prosba o pozyczke rozpatrzona pozytywnie",
                Body = String.Format("Drogi/Droga {0} {1},\n\nTwoja prosba o pozyczke zostala rozpatrzona pomyslnie.\n" +
                "Szegoly to:\n" +
                "kwota: {2}\n" +
                "oprocentowanie: {3}" +
                "liczba rat: {4}", name, surname, quanity, interest, installment)
            };
            try
            {
                client.Send(mail);
                Console.WriteLine("Mail został wysłany");
            }
            catch (Exception e)
            {
                Console.WriteLine(e.ToString());
            }
            
        }
    }
}
