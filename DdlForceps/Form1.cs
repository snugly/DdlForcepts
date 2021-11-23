using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Data;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Management.Automation;
using System.Text;
using System.Text.RegularExpressions;
using System.Windows.Forms;

using Dapper;
using Dapper.Oracle;
using LibGit2Sharp;
using Oracle.ManagedDataAccess.Client;
using Oracle.ManagedDataAccess.Types;

namespace DdlForceps
{
    public partial class Form1 : Form
    {
        // TODO: рефакторинг
        // TODO: конфиг
        // TODO: асинхронная загруза
        // TODO: список фиксов
        // TODO: список файлов в фиксах и их сравнение
        // TODO: добавление тэгов
        // TODO: выбор файлов для сборки. создание ветки, сборка старой версии, фикс, сборка новой версии, фикс
        // TODO: пуш и мерж
        
        private static readonly Regex invalidPathRegex = new Regex($"[{new string(Path.GetInvalidPathChars())}]", RegexOptions.Compiled);
        private static readonly Regex invalidFileNameRegex = new Regex($"[{new string(Path.GetInvalidFileNameChars())}]", RegexOptions.Compiled);

        private static readonly HashSet<string> objectTypes = new HashSet<string> {
            "TABLE",
            "TRIGGER",
            "INDEX",
            "SYNONYM",
            "SEQUENCE",
            "VIEW",

            "FUNCTION",
            "PROCEDURE",
            "PACKAGE",
            "PACKAGE_BODY",
            "TYPE",
            "TYPE_BODY"
        };

        private static readonly HashSet<string> schemas = new HashSet<string> {
        };

        private static readonly string getObjectsSql = @$"
SELECT
    O.owner,
    O.object_name as name,
    replace(O.object_type, ' ', '_') as type
FROM all_objects O
WHERE O.generated = 'N'
  AND O.secondary = 'N'
  AND O.oracle_maintained = 'N'
  AND O.owner IN ('{string.Join("','", Form1.schemas)}')
  AND last_ddl_time > :last
ORDER BY last_ddl_time DESC";

        private static readonly string getDdlSql =
@"
declare
    ddl_code clob;
begin
    ddl_code := to_clob(trim(trim(chr(10) from DBMS_METADATA.GET_DDL(:type, :name, :owner))));
    :ddl_code := ddl_code;
end;
";
/*
begin
:ddl_code := trim(trim(chr(10) from DBMS_METADATA.GET_DDL(:type, :name, :owner)));
end;
*/

        private static readonly string RFC2822Format = "ddd dd MMM HH:mm:ss yyyy K";

        private static readonly string dateFormat = "yyyy-MM-ddTHH:mm:ss";

        public Form1()
        {
            InitializeComponent();
        }

        private void AppendLine(string? line = null)
        {
            if (line != null)
            {
                this.richTextBox1.AppendText(line);
            }
            this.richTextBox1.AppendText(Environment.NewLine);
        }

        private void Form1_Load(object sender, EventArgs e)
        {
        }

        public void LoadDdl(DateTime last)
        {
            using (OracleConnection cn = new OracleConnection(Form1.connectionString))
            {
                cn.Open();
                var objects = this.GetObjects(cn, last);

                foreach (var obj in objects)
                {
                    if (Form1.objectTypes.Contains(obj.Type))
                    {
                        try
                        {
                            var ddl = this.GetDdl(cn, obj);
                            this.Save(obj, ddl);
                        }
                        // object not found
                        catch (OracleException ex) when (ex.Number == 31603)
                        {
                            this.AppendLine($"{obj.Type} {obj.Owner}.{obj.Name} не найден");
                        }
                    }
                }
            }

            this.Commit(folder, DateTime.Now.ToString(Form1.dateFormat));
        }

        public DateTime GetLastAutocommitDate()
        {
            using (var repo = new Repository(folder))
            {
                foreach (Commit c in repo.Commits.Take(15))
                {
                    DateTime dt;
                    if (DateTime.TryParseExact(c.Message?.Substring(0, Form1.dateFormat.Length), Form1.dateFormat, null, DateTimeStyles.None, out dt))
                    {
                        return dt;
                    }
                }
            }

            return new DateTime(2000, 1, 1);
        }

        public void ShowChangeLog()
        {
            using (var repo = new Repository(folder))
            {
                //repo.ApplyTag("vNew", repo.Commits.First().Sha);
                Commit lastCommit = null;
                Commit lastTagged = null;

                foreach (Commit c in repo.Commits.Take(15))
                {
                    if (lastCommit == null)
                    {
                        lastCommit = c;
                    }

                    this.AppendLine($"commit {c.Id}");

                    if (c.Parents.Count() > 1)
                    {
                        this.AppendLine($"Merge: {string.Join(" ", c.Parents.Select(p => p.Id.Sha.Substring(0, 7)).ToArray())}");
                    }

                    this.AppendLine(string.Format($"Author: {c.Author.Name} <{c.Author.Email}>"));
                    this.AppendLine($"Date:   {c.Author.When.ToString(Form1.RFC2822Format, CultureInfo.InvariantCulture)}");

                    var tag = repo.Tags.Where(t => t.Target == c).FirstOrDefault();

                    if (tag != null)
                    {
                        if (lastTagged == null && c != lastCommit)
                        {
                            lastTagged = c;
                        }

                        this.AppendLine(tag.FriendlyName);
                    }

                    this.AppendLine();
                    this.AppendLine(c.Message);
                    this.AppendLine();

                    if (lastCommit != null && lastTagged != null)
                    {
                        var p = repo.Diff.Compare<Patch>(lastCommit.Tree, lastTagged.Tree);

                        foreach (var a in p)
                        {
                            this.AppendLine(a.Path);
                        }
                    }
                }
            }
        }

        public void Commit(string folder, string comment)
        {
            using (var ps = PowerShell.Create())
            {
                ps.AddScript($"cd \"{folder}\"");

                //ps.AddScript(@"git init");
                ps.AddScript(@"git add -A");
                ps.AddScript(@$"git commit -m '{comment.Replace("'", "'\''")}'");
                ps.AddScript(@"git push");

                ps.Invoke();
            }
        }

        private void Save(ServerObject obj, string ddl)
        {
            if (ddl is null)
            {
                return;
            }

            string path = Path.Combine(Form1.folder, Form1.invalidPathRegex.Replace(obj.Owner, "_"), Form1.invalidPathRegex.Replace(obj.Type, "_"));
            Directory.CreateDirectory(path);

            string fileName = Path.Combine(path, $"{Form1.invalidFileNameRegex.Replace(obj.Name, "_")}.sql");

            File.WriteAllText(fileName, ddl, Encoding.UTF8);
        }

        private IEnumerable<ServerObject> GetObjects(IDbConnection cn, DateTime last)
        {
            return cn.Query<ServerObject>(getObjectsSql, new { last = last });
        }

        private string GetDdl(IDbConnection cn, ServerObject obj)
        {
            OracleDynamicParameters param = new();
            param.Add("ddl_code", null, direction: ParameterDirection.Output, dbType: OracleMappingType.Clob);
            param.AddDynamicParams(obj);
            
            cn.Execute(Form1.getDdlSql, param);

            var ddl = param.GetParameter("ddl_code").AttachedParam.Value as OracleClob;
            return ddl?.Value;
        }

        private class ServerObject
        {
            public string Owner { get; set; }
            public string Name { get; set; }
            public string Type { get; set; }
        }

        private void button1_Click(object sender, EventArgs e)
        {
            ShowChangeLog();
        }

        private void button2_Click(object sender, EventArgs e)
        {
            LoadDdl(GetLastAutocommitDate());
        }

        private void button3_Click(object sender, EventArgs e)
        {
            ClearOutput();
        }

        private void ClearOutput()
        {
            this.richTextBox1.Text = "";
        }
    }
}
