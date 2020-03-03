using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SQLite;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using SqliteHelper;
namespace System.Data.SQLite.SQLiteHelper
{
    public class SQLiteHelper
    {
        
        /// <summary>
        /// 数据读取定义
        /// </summary>
        /// 
        //private SQLiteDataReader dataReader;
        private Common.DbTransaction DBtrans;
        private static readonly Dictionary<string, ClsLock> RWL=new Dictionary<string, ClsLock>();
        /// <summary>
        /// 数据库地址
        /// </summary>
        private readonly string mdataFile;

        private readonly string mPassWord;
        private readonly string LockName = null;
        /// <summary>
        /// 数据库连接定义
        /// </summary>
        private SQLiteConnection mConn;

        /// <summary>
        /// 根据数据库地址初始化
        /// </summary>
        /// <param name="dataFile">数据库地址</param>
        public SQLiteHelper(string dataFile)
        {
            if (!RWL.ContainsKey(dataFile))
            {
                LockName = dataFile;
                RWL.Add(dataFile, new ClsLock());
            }
            this.mdataFile = dataFile ?? throw new ArgumentNullException("dataFile=null");
            this.mdataFile = AppDomain.CurrentDomain.BaseDirectory + dataFile;
        }
        /// <summary>
        /// 使用密码打开数据库
        /// </summary>
        /// <param name="dataFile">数据库地址</param>
        /// <param name="PassWord">数据库密码</param>
        public SQLiteHelper(string dataFile, string PassWord)
        {
            if (!RWL.ContainsKey(dataFile))
            {
                LockName = dataFile;
                RWL.Add(dataFile, new ClsLock());
            }
            this.mdataFile = dataFile ?? throw new ArgumentNullException("dataFile is null");
            this.mPassWord = PassWord ?? throw new ArgumentNullException("PassWord is null");
            this.mdataFile = AppDomain.CurrentDomain.BaseDirectory + dataFile;
        }
        /// <summary>  
        /// <para>打开SQLiteManager使用的数据库连接</para>  
        /// </summary>  
        public void Open()
        {
            if (string.IsNullOrWhiteSpace(mPassWord))
            {
                mConn = OpenConnection(this.mdataFile);
            }
            else
            {
                mConn = OpenConnection(this.mdataFile, mPassWord);
            }

        }
        public void BeginTrain()
        {
            EnsureConnection();
            DBtrans = mConn.BeginTransaction();
        }
        public void DBCommit()
        {
            try
            {
                DBtrans.Commit();
            }
            catch (Exception)
            {
                DBtrans.Rollback();
            }
        }
 
        /// <summary>  
        /// <para>安静地关闭连接,保存不抛出任何异常</para>  
        /// </summary>  
        public void Close()
        {
            if (this.mConn != null)
            {
                try
                {
                    this.mConn.Close();
                    if (RWL.ContainsKey(LockName))
                    {
                        
                        RWL.Remove(LockName);
                    }
                    
                }
                catch
                {
                }
            
            }
        }

        /// <summary>  
        /// <para>创建一个连接到指定数据文件的SQLiteConnection,并Open</para>  
        /// <para>如果文件不存在,创建之</para>  
        /// </summary>  
        /// <param name="dataFile"></param>  
        /// <returns></returns>  
        private SQLiteConnection OpenConnection(string dataFile)
        {

            if (dataFile == null)
            {
                throw new ArgumentNullException("dataFiledataFile=null");
            }

            if (!File.Exists(dataFile))
            {
                SQLiteConnection.CreateFile(dataFile);
            }

            SQLiteConnection conn = new SQLiteConnection();
            SQLiteConnectionStringBuilder conStr = new SQLiteConnectionStringBuilder
            {
                DataSource = dataFile
            };
            conn.ConnectionString = conStr.ToString();
            conn.Open();

            return conn;

        }
        /// <summary>  
        /// <para>创建一个连接到指定数据文件的SQLiteConnection,并Open</para>  
        /// <para>如果文件不存在,创建之</para>  
        /// </summary>  
        /// <param name="dataFile"></param>  
        /// <returns></returns>  
        private SQLiteConnection OpenConnection(string dataFile, string Password)
        {
            if (dataFile == null)
            {
                throw new ArgumentNullException("dataFile=null");
            }

            if (!File.Exists(Convert.ToString(dataFile)))
            {
                SQLiteConnection.CreateFile(dataFile);
            }
            try
            {
                SQLiteConnection conn = new SQLiteConnection();
                SQLiteConnectionStringBuilder conStr = new SQLiteConnectionStringBuilder
                {
                    DataSource = dataFile,
                    Password = Password
                };
                conn.ConnectionString = conStr.ToString();
                conn.Open();
                return conn;

            }
            catch (Exception)
            {
                return null;
            }

        }

        /// <summary>  
        /// <para>读取或设置SQLiteManager使用的数据库连接</para>  
        /// </summary>  
        public SQLiteConnection Connection
        {
            get
            {
                return this.mConn;
            }
            set
            {
                this.mConn = value ?? throw new ArgumentNullException();
            }
        }

        protected void EnsureConnection()
        {
            if (this.mConn == null)
            {
                throw new Exception("SQLiteManager.Connection=null");
            }

            if (mConn.State != ConnectionState.Open)
            {
                mConn.Open();

            }

        }

        public string GetDataFile()
        {
            return this.mdataFile;
        }

        /// <summary>  
        /// <para>判断表table是否存在</para>  
        /// </summary>  
        /// <param name="table"></param>  
        /// <returns></returns>  
        public bool TableExists(string table)
        {
            if (table == null)
            {
                throw new ArgumentNullException("table=null");
            }
            EnsureConnection();
            SQLiteDataReader reader = ExecuteReader("SELECT count(*) as c FROM sqlite_master WHERE type='table' AND name=@tableName ", new SQLiteParameter[] { new SQLiteParameter("tableName", table) });
            if (reader == null)
            {
                return false;
            }
            reader.Read();
            int c = reader.GetInt32(0);
            reader.Close();
            reader.Dispose();
            //return false;  
            return c == 1;
        }

        ///<summary>
        ///<para> 执行SQL, 并返回SQLiteDataReader对象结果</para>  
        /// </summary>  
        /// <param name = "sql" ></param>
        ///<param name = "paramArr" > null 表示无参数</param>  
        /// <returns></returns>  
        public SQLiteDataReader ExecuteReader(string sql, SQLiteParameter[] paramArr)
        {
            if (sql == null)
            {
                throw new ArgumentNullException("sql=null");
            }
            EnsureConnection();

            using (SQLiteCommand cmd = new SQLiteCommand(sql, Connection))
            {
                if (paramArr != null)
                {
                    cmd.Parameters.AddRange(paramArr);
                }
                try
                {
                    SQLiteDataReader reader = cmd.ExecuteReader();
                    cmd.Parameters.Clear();
                    return reader;
                }
                catch (Exception)
                {

                    return null;
                }
            }


        }
        /// <summary>
        /// 执行查询，并返回dataset对象
        /// </summary>
        /// <param name="sql">SQL查询语句</param>
        /// <param name="paramArr">参数数组</param>
        /// <returns></returns>
        public DataSet ExecuteDataSet(string sql, SQLiteParameter[] paramArr)
        {
            if (sql == null)
            {
                throw new ArgumentNullException("sql=null");
            }
            this.EnsureConnection();

            using (SQLiteCommand cmd = new SQLiteCommand(sql, this.Connection))
            {
                if (paramArr != null)
                {
                    cmd.Parameters.AddRange(paramArr);
                }
                try
                {
                    SQLiteDataAdapter da = new SQLiteDataAdapter();
                    DataSet ds = new DataSet();
                    da.SelectCommand = cmd;
                    da.Fill(ds);
                    cmd.Parameters.Clear();
                    da.Dispose();
                    return ds;
                }
                catch (Exception)
                {
                    return null;
                }
            }


        }

        /// <summary>
        /// 执行SQL查询，并返回dataset对象。
        /// </summary>
        /// <param name="strTable">映射源表的名称</param>
        /// <param name="sql">SQL语句</param>
        /// <param name="paramArr">SQL参数数组</param>
        /// <returns></returns>
        public DataSet ExecuteDataSet(string strTable, string sql, SQLiteParameter[] paramArr)
        {
            if (sql == null)
            {
                throw new ArgumentNullException("sql=null");
            }
            this.EnsureConnection();

            using (SQLiteCommand cmd = new SQLiteCommand(sql, this.Connection))
            {
                if (paramArr != null)
                {
                    cmd.Parameters.AddRange(paramArr);
                }
                try
                {

                    SQLiteDataAdapter da = new SQLiteDataAdapter();
                    DataSet ds = new DataSet();
                    da.SelectCommand = cmd;
                    da.Fill(ds, strTable);
                    cmd.Parameters.Clear();
                    da.Dispose();
                    return ds;
                }
                catch (Exception)
                {
                    return null;
                }
            }
        }

        /// <summary>  
        /// <para>执行SQL,返回结果集第一行</para>  
        /// <para>如果结果集为空,那么返回空List (List.Count=0)</para>  
        /// <para>rowWrapper = null时,使用WrapRowToDictionary</para>  
        /// </summary>  
        /// <param name="sql"></param>  
        /// <param name="paramArr"></param>  
        /// <returns></returns>  
        public object ExecuteScalar(string sql, SQLiteParameter[] paramArr)
        {
            if (sql == null)
            {
                throw new ArgumentNullException("sql=null");
            }
            this.EnsureConnection();

            using (SQLiteCommand cmd = new SQLiteCommand(sql, Connection))
            {
                if (paramArr != null)
                {

                    cmd.Parameters.AddRange(paramArr);

                }
                try
                {
                    object reader = cmd.ExecuteScalar();
                    cmd.Parameters.Clear();
                    cmd.Dispose();
                    return reader;
                }
                catch (Exception)
                {
                    return null;
                }
            }

        }

        #region 增删改
        /// <summary>  
        /// <para>执行SQL,返回受影响的行数</para>  
        /// <para>可用于执行表创建语句</para>  
        /// <para>paramArr == null 表示无参数</para>  
        /// </summary>  
        /// <param name="sql"></param>  
        /// <returns></returns>  
        public int ExecuteNonQuery(string sql, SQLiteParameter[] paramArr)
        {
            if (sql == null)
            {
                throw new ArgumentNullException("sql=null");
            }
            this.EnsureConnection();
            //Dim mReaderWriterLock As New Threading.ReaderWriterLock
            //mReaderWriterLock.AcquireWriterLock(3000)
            try
            {
                using (SQLiteCommand cmd = new SQLiteCommand(sql, Connection))
                {
                    if (paramArr != null)
                    {
                        foreach (SQLiteParameter p in paramArr)
                        {
                            cmd.Parameters.Add(p);
                        }
                    }
                    int c = cmd.ExecuteNonQuery();
                    cmd.Parameters.Clear();
                    return c;
                }
            }
            catch (SQLiteException)
            {
                return 0;
            }
            finally
            {
                //mReaderWriterLock.ReleaseLock()
            }

        }
        /// <summary>  
        /// <para>执行insert into语句</para>  
        /// </summary>  
        /// <param name="table"></param>  
        /// <param name="entity"></param>  
        /// <returns></returns>  
        public int Save(string table, Dictionary<string, object> entity)
        {
            if (table == null)
            {
                throw new ArgumentNullException("table=null");
            }
            this.EnsureConnection();
            string sql = BuildInsert(table, entity);
            return this.ExecuteNonQuery(sql, BuildParamArray(entity));
        }

        private SQLiteParameter[] BuildParamArray(Dictionary<string, object> entity)
        {
            List<SQLiteParameter> list = new List<SQLiteParameter>();
            foreach (string key in entity.Keys)
            {
                list.Add(new SQLiteParameter(key, entity[key]));
            }
            if (list.Count == 0)
            {
                return null;
            }
            return list.ToArray();
        }

        private string BuildInsert(string table, Dictionary<string, object> entity)
        {
            StringBuilder buf = new StringBuilder();
            buf.Append("insert into ").Append(table);
            buf.Append(" (");
            foreach (string key in entity.Keys)
            {
                buf.Append(key).Append(",");
            }
            buf.Remove(buf.Length - 1, 1); // 移除最后一个,
            buf.Append(") ");
            buf.Append("values(");
            foreach (string key in entity.Keys)
            {
                buf.Append("@").Append(key).Append(","); // 创建一个参数
            }
            buf.Remove(buf.Length - 1, 1);
            buf.Append(") ");

            return buf.ToString();
        }

        private string BuildUpdate(string table, Dictionary<string, object> entity)
        {
            StringBuilder buf = new StringBuilder();
            buf.Append("update ").Append(table).Append(" set ");
            foreach (string key in entity.Keys)
            {
                buf.Append(key).Append("=").Append("@").Append(key).Append(",");
            }
            buf.Remove(buf.Length - 1, 1);
            buf.Append(" ");
            return buf.ToString();
        }

        /// <summary>  
        /// <para>执行update语句</para>  
        /// <para>where参数不必要包含'where'关键字</para>  
        ///   
        /// <para>如果where=null,那么忽略whereParams</para>  
        /// <para>如果where!=null,whereParams=null,where部分无参数</para>  
        /// </summary>  
        /// <param name="table"></param>  
        /// <param name="entity"></param>  
        /// <param name="where"></param>  
        /// <param name="whereParams"></param>  
        /// <returns></returns>  
        public int Update(string table, Dictionary<string, object> entity, string where, SQLiteParameter[] whereParams)
        {
            if (table == null)
            {
                throw new ArgumentNullException("table=null");
            }
            this.EnsureConnection();
            string sql = BuildUpdate(table, entity);
            SQLiteParameter[] arr = BuildParamArray(entity);
            if (where != null)
            {
                sql += " where " + where;
                if (whereParams != null)
                {
                    SQLiteParameter[] newArr = new SQLiteParameter[(arr.Length + whereParams.Length)];
                    Array.Copy(arr, newArr, arr.Length);
                    Array.Copy(whereParams, 0, newArr, arr.Length, whereParams.Length);

                    arr = newArr;
                }
            }
            return this.ExecuteNonQuery(sql, arr);
        }
        /// <summary>  
        /// 执行delete from table 语句  
        /// where不必包含'where'关键字  
        /// where=null时将忽略whereParams  
        /// </summary>  
        /// <param name="table"></param>  
        /// <param name="where"></param>  
        /// <param name="whereParams"></param>  
        /// <returns></returns>  
        public int Delete(string table, string where, SQLiteParameter[] whereParams)
        {
            if (table == null)
            {
                throw new ArgumentNullException("table=null");
            }
            this.EnsureConnection();
            string sql = "delete from " + table + " ";
            if (where != null)
            {
                sql += "where " + where;
            }
            return ExecuteNonQuery(sql, whereParams);
        }

        public bool CheckExists(string table, string conditionCol, object conditionVal)
        {
            object isExists = QueryOne(table, conditionCol, conditionVal);
            if (isExists == null)
            {
                return false;
            }
            else
            {
                return true;
            }

        }
        #endregion


        /// <summary>  
        /// <para>查询一行记录,无结果时返回null</para>  
        /// <para>conditionCol = null时将忽略条件,直接执行select * from table </para>  
        /// </summary>  
        /// <param name="table"></param>  
        /// <param name="conditionCol"></param>  
        /// <param name="conditionVal"></param>  
        /// <returns></returns>  
        public object QueryOne(string table, string conditionCol, object conditionVal)
        {
            if (table == null)
            {
                throw new ArgumentNullException("table=null");
            }
            this.EnsureConnection();

            string sql = "select * from " + table;
            if (conditionCol != null)
            {
                sql += " where " + conditionCol + "=@" + conditionCol;
            }
        

            object result = ExecuteScalar(sql, new SQLiteParameter[] { new SQLiteParameter(conditionCol, conditionVal) });
            return result;
        }

        public bool Vacuum()
        {
            try
            {
                using (SQLiteCommand Command = new SQLiteCommand("VACUUM", Connection))
                {
                    Command.ExecuteNonQuery();
                }
                return true;
            }
            catch (System.Data.SQLite.SQLiteException)
            {
                return false;
            }

        }

    }

}
