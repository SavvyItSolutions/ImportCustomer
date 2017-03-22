using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Data;
using System.Text;
using System.Threading.Tasks;
using System.Configuration;
using System.Data.SqlClient;
using System.Xml;
using System.Net;

namespace ImportCustomer
{
    public class Program
    {
        public static void Main(string[] args)
        {
            string username = System.Configuration.ConfigurationManager.AppSettings["userName"];
            string password = System.Configuration.ConfigurationManager.AppSettings["password"];
            string ftploc = System.Configuration.ConfigurationManager.AppSettings["ftpSourceFilePath"];
            string Destloc = System.Configuration.ConfigurationManager.AppSettings["localDestinationFilePath"];
            DownloadFile(username, password, ftploc, Destloc);
            var filename = Destloc;
            var reader = ReadAsLines(filename);

            var data = new DataTable();

            //this assume the first record is filled with the column names
            //var headers = reader.First().Split('\t');
            List<string> headers = new List<string>();
            headers.Add("CustomerID");
            headers.Add("FirstName");
            headers.Add("LastName");
            headers.Add("PhoneNumber");
            headers.Add("Phone2");
            headers.Add("Email");
            headers.Add("Address1");
            headers.Add("Address2");
            headers.Add("City");
            headers.Add("State");
            headers.Add("CustomerType");
            headers.Add("CustomerAdded");
            headers.Add("CardNumber");
            headers.Add("Notes1");
            headers.Add("IsUpdated");
            headers.Add("LastUpdatedOn");

            foreach (var header in headers)
            {
                data.Columns.Add(header);
            }



            var records = reader.Skip(0);
            foreach (var record in records)
            {
                data.Rows.Add(record.Split('\t'));
            }

            if (data.Rows.Count > 1)
            {

                XmlDocument xmlData = ConvertDataTableToXML(data);
                DataSet ds = new DataSet();
                SqlDataAdapter da = new SqlDataAdapter();
                string str = ConfigurationManager.ConnectionStrings["DBCON"].ConnectionString;
                try
                {
                    using (SqlConnection con = new SqlConnection(str))
                    {
                        using (SqlCommand cmd = new SqlCommand("ImportCustomer", con))
                        {
                            cmd.CommandType = CommandType.StoredProcedure;
                            cmd.Parameters.Add("@xmlDoc", SqlDbType.NVarChar).Value = xmlData.InnerXml;
                            cmd.Connection = con;
                            cmd.CommandTimeout = 1000;
                            con.Open();
                            da.SelectCommand = cmd;
                            da.Fill(ds);
                            DataRow dr = ds.Tables[0].Rows[0];
                            DailyStats(dr);
                            con.Close();
                        }

                    }
                }
                catch (Exception ex)
                {
                    LogErro(ex);
                }
            }


        }

        static IEnumerable<string> ReadAsLines(string filename)
        {
            using (var reader = new StreamReader(filename))
                while (!reader.EndOfStream)
                    yield return reader.ReadLine();
        }

        private static XmlDocument ConvertDataTableToXML(DataTable dtData)
        {
            XmlDocument xdoc = new XmlDocument();
            XmlElement rootNode = xdoc.CreateElement("Customer");
            foreach(DataRow dr in dtData.Rows)
            {
                XmlElement childNode = xdoc.CreateElement("CustomerData");
                childNode.SetAttribute("CustomerID", dr["CustomerID"].ToString());
                childNode.SetAttribute("FirstName", dr["FirstName"].ToString());
                childNode.SetAttribute("LastName", dr["LastName"].ToString());
                childNode.SetAttribute("PhoneNumber", dr["PhoneNumber"].ToString());
                childNode.SetAttribute("Phone2", dr["Phone2"].ToString());
                childNode.SetAttribute("Email", dr["Email"].ToString());
                childNode.SetAttribute("Address1", dr["Address1"].ToString());
                childNode.SetAttribute("Address2", dr["Address2"].ToString());
                childNode.SetAttribute("City", dr["City"].ToString());
                childNode.SetAttribute("State", dr["State"].ToString());
                childNode.SetAttribute("CustomerType", dr["CustomerType"].ToString());
                childNode.SetAttribute("CustomerAdded", dr["CustomerAdded"].ToString());
                childNode.SetAttribute("CardNumber", dr["CardNumber"].ToString());
                childNode.SetAttribute("Notes1", dr["Notes1"].ToString());
                childNode.SetAttribute("IsUpdated", DBNull.Value.ToString());
                childNode.SetAttribute("LastUpdatedOn", DBNull.Value.ToString());
                rootNode.AppendChild(childNode);
            }
            xdoc.AppendChild(rootNode);

            return xdoc;
        }

        private static void LogErro(Exception ex)
        {
            string message = string.Format("Time: {0}", DateTime.Now.ToString("dd/MM/yyyy hh:mm:ss tt"));
            message += Environment.NewLine;
            message += "-----------------------------------------------------------";
            message += Environment.NewLine;
            message += string.Format("Message: {0}", ex.Message);
            message += Environment.NewLine;
            message += string.Format("StackTrace: {0}", ex.StackTrace);
            message += Environment.NewLine;
            message += string.Format("Source: {0}", ex.Source);
            message += Environment.NewLine;
            message += string.Format("TargetSite: {0}", ex.TargetSite.ToString());
            message += Environment.NewLine;
            message += "-----------------------------------------------------------";
            message += Environment.NewLine;
            string path = @"C:\soumik\Savvy\ImportCustomer\ErrorLog\";
            System.IO.Directory.CreateDirectory(path);
            using (StreamWriter writer = new StreamWriter(path+"Error.txt", true))
            {
                writer.WriteLine(message);
                writer.Close();
            }
        }
        private static void DownloadFile(string userName, string password, string ftpSourceFilePath, string localDestinationFilePath)
        {
            int bytesRead = 0;
            byte[] buffer = new byte[2048];

            FtpWebRequest request = CreateFtpWebRequest(ftpSourceFilePath, userName, password, true);
            request.Method = WebRequestMethods.Ftp.DownloadFile;

            Stream reader = request.GetResponse().GetResponseStream();
            FileStream fileStream = new FileStream(localDestinationFilePath, FileMode.Create);

            while (true)
            {
                bytesRead = reader.Read(buffer, 0, buffer.Length);

                if (bytesRead == 0)
                    break;

                fileStream.Write(buffer, 0, bytesRead);
            }
            fileStream.Close();
        }

        private static FtpWebRequest CreateFtpWebRequest(string ftpDirectoryPath, string userName, string password, bool keepAlive = false)
        {
            FtpWebRequest request = (FtpWebRequest)WebRequest.Create(new Uri(ftpDirectoryPath));

            //Set proxy to null. Under current configuration if this option is not set then the proxy that is used will get an html response from the web content gateway (firewall monitoring system)
            request.Proxy = null;

            request.UsePassive = true;
            request.UseBinary = true;
            request.KeepAlive = keepAlive;

            request.Credentials = new NetworkCredential(userName, password);

            return request;
        }
        private static void DailyStats(DataRow dr)
        {
            string message = string.Format("Time: {0}", DateTime.Now.ToString("dd/MM/yyyy hh:mm:ss tt"));
            message += Environment.NewLine;
            message += "-----------------------------------------------------------";
            message += Environment.NewLine;
            message += "No of records inserted : " + dr["inserted"];
            message += Environment.NewLine;
            message += "No of records updated : " + dr["updated"];
            message += Environment.NewLine;
            message += "-----------------------------------------------------------";
            message += Environment.NewLine;
            string path = ConfigurationManager.AppSettings["Statsdest"];
            System.IO.Directory.CreateDirectory(path);
            using (StreamWriter writer = new StreamWriter(path + "Stats.txt", true))
            {
                writer.WriteLine(message);
                writer.Close();
            }
        }
    }
}
