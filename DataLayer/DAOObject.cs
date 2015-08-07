using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.Data.SqlClient;
using System.Configuration;


namespace DBAccess
{

    public class DAOObject
    {

        public DAOObject()
        {
        }

        #region "Connection String"


        private string mCN;

        public DAOObject(string ConnectionString)
        {
            //Permite usar un ConnectionString diferente al del WebConfig
            mCN = ConnectionString;

        }

        public SqlConnection GetConnection()
        {

            SqlConnection ret_conn = null;


            if (mCN == null)
            {
                //Usa la Conección del WebConfig
                ret_conn = new SqlConnection(ConfigurationManager.ConnectionStrings["DBConn"].ConnectionString);


            }
            else
            {
                //Usa la Conección Provista
                ret_conn = new SqlConnection(mCN);

            }

            ret_conn.Open();
            return ret_conn;

        }


        public void CloseConnection(SqlConnection conn)
        {
            conn.Close();
            conn = null;

        }

        #endregion

        #region "ExecuteDataReader"

        private SqlDataReader ExecuteDataReader(string CommandText, CommandType CommandType, List<SqlParameter> ParameterValues = null, SqlConnection cn = null)
        {

            SqlParameter Parameter = null;
            SqlCommand cmd = new SqlCommand(CommandText, cn);
            SqlDataReader Reader;


            try
            {
                cmd.CommandType = CommandType;
                 

                if ((ParameterValues != null))
                {
                    foreach (SqlParameter Parameter_loopVariable in ParameterValues)
                    {
                        Parameter = Parameter_loopVariable;
                        cmd.Parameters.Add(Parameter);
                    }
                }

                Reader = cmd.ExecuteReader();


            }
            catch (SqlException ex)
            {
                string msg = null;
                msg = ex.Message;

                throw new Exception(msg);

            }
            finally
            {
                if (ParameterValues != null)
                {
                    ParameterValues.Clear();
                }
                


            }

            return Reader;

        }

        #endregion

        #region "ExecuteNonQuery"

        private int ExecuteNonQuery(string CommandText, CommandType CommandType, List<SqlParameter> ParameterValues)
        {

            SqlConnection cn = GetConnection();
            int res = 0;
            SqlParameter Parameter = null;
            SqlCommand cmd = new SqlCommand(CommandText, cn);


            try
            {
                cmd.CommandType = CommandType;


                if ((ParameterValues != null))
                {
                    foreach (SqlParameter Parameter_loopVariable in ParameterValues)
                    {
                        Parameter = Parameter_loopVariable;
                        cmd.Parameters.Add(Parameter);
                    }

                }

                res = cmd.ExecuteNonQuery();


            }
            catch (SqlException ex)
            {
                string msg = null;
                msg = ex.Message;

                throw new Exception(msg);


            }
            finally
            {
                if (ParameterValues != null)
                {
                    ParameterValues.Clear();
                }
                CloseConnection(cn);
                cmd.Dispose();

            }

            return res;

        }
        #endregion

        #region "ExecuteScalar"

        private int ExecuteScalar(string CommandText, CommandType CommandType, List<SqlParameter> ParameterValues = null)
        {

            SqlConnection cn = GetConnection();
            int res = 0;
            SqlParameter Parameter = null;
            SqlCommand cmd = new SqlCommand(CommandText, cn);


            try
            {
                cmd.CommandType = CommandType;


                if ((ParameterValues != null))
                {
                    foreach (SqlParameter Parameter_loopVariable in ParameterValues)
                    {
                        Parameter = Parameter_loopVariable;
                        cmd.Parameters.Add(Parameter);
                    }

                }

                res = (int)cmd.ExecuteScalar();


            }
            catch (SqlException ex)
            {
                string msg = null;
                msg = ex.Message;

                throw new Exception(msg);


            }
            finally
            {
                if (ParameterValues != null)
                {
                    ParameterValues.Clear();
                }
                cmd.Dispose();
                CloseConnection(cn);

            }

            return res;

        }

        #endregion

        #region "ExecuteReader"

        private DataSet ExecuteReader(string CommandText, CommandType CommandType, List<SqlParameter> ParameterValues = null)
        {


            SqlConnection cn = GetConnection();
            SqlParameter Parameter = null;
            SqlCommand cmd = new SqlCommand(CommandText, cn);
            SqlDataAdapter adapter = new SqlDataAdapter();
            DataSet ds = new DataSet();


            try
            {
                cmd.CommandType = CommandType;
                adapter.SelectCommand = cmd;

                if ((ParameterValues != null))
                {
                    foreach (SqlParameter Parameter_loopVariable in ParameterValues)
                    {
                        Parameter = Parameter_loopVariable;
                        cmd.Parameters.Add(Parameter);
                    }
                }

                adapter.Fill(ds);


            }
            catch (SqlException ex)
            {
                string msg = null;
                msg = ex.Message;

                throw new Exception(msg);

            }
            finally
            {
                if (ParameterValues != null)
                {
                    ParameterValues.Clear();
                }
                cmd.Dispose();
                adapter.Dispose();
                CloseConnection(cn);


            }

            return ds;

        }

        #endregion

        #region "Return Procedure"

        public DataTable RunSPReturnDT(string CommandText, CommandType CommandType, List<SqlParameter> ParameterValues = null)
        {
            DataSet DS = new DataSet();

            DataTable DT = new DataTable();

            try
            {

                DS = ExecuteReader(CommandText, CommandType, ParameterValues);
                DT = DS.Tables[0];


            }
            catch (SqlException ex)
            {
                string msg = null;
                msg = ex.Message;

                throw new Exception(msg);


            }
            finally
            {
                if (ParameterValues != null)
                {
                    ParameterValues.Clear();
                }
                DS.Tables[0].Dispose();
                DS.Dispose();

            }

            return DT;

        }

        public DataSet RunSPReturnDataSetWithMultiDT(string CommandText, CommandType CommandType, List<string> DataTablesNames, List<SqlParameter> ParameterValues = null)
        {

            DataSet DS = new DataSet();
            int Count = 0;
            Count = -1;


            try
            {
                DS = ExecuteReader(CommandText, CommandType, ParameterValues);


                foreach (DataTable DT in DS.Tables)
                {
                    Count = Count + 1;

                    DT.TableName = DataTablesNames[Count].ToString();

                }


            }
            finally
            {
                if (DataTablesNames != null)
                {
                    DataTablesNames.Clear();
                }
                if (ParameterValues != null)
                {
                    ParameterValues.Clear();
                }

            }

            return DS;

        }

        public SqlDataReader RunSPReturnDR(string CommandText, CommandType CommandType, List<SqlParameter> ParameterValues = null, SqlConnection cn = null)
        {
            SqlDataReader Reader;

            try
            {

                Reader = ExecuteDataReader(CommandText, CommandType, ParameterValues, cn);
                
            }
            catch (SqlException ex)
            {
                string msg = null;
                msg = ex.Message;

                throw new Exception(msg);


            }
            finally
            {
                if (ParameterValues != null)
                {
                    ParameterValues.Clear();
                }
               

            }

            return Reader;
        }

        #endregion

        #region "Insert Procedure"

        public int RunActionQuery(string CommandText, CommandType CommandType, List<SqlParameter> ParameterValues)
        {

            int res = 0;


            try
            {
                res = ExecuteNonQuery(CommandText, CommandType, ParameterValues);


            }
            catch (SqlException ex)
            {
                string msg = null;
                msg = ex.Message;

                throw new Exception(msg);


            }
            finally
            {
                if (ParameterValues != null)
                {
                    ParameterValues.Clear();
                }

            }

            return res;

        }

        #endregion

        #region "Scalar Procedure"

        public int RunSPReturnInteger(string CommandText, CommandType CommandType, List<SqlParameter> ParameterValues = null)
        {
            int res = 0;


            try
            {
                res = ExecuteScalar(CommandText, CommandType, ParameterValues);


            }
            catch (SqlException ex)
            {
                string msg = null;
                msg = ex.Message;

                throw new Exception(msg);


            }
            finally
            {
                if (ParameterValues != null)
                {
                    ParameterValues.Clear();
                }

            }

            return res;

        }

        #endregion

    }

}

