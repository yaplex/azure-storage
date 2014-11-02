using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Reflection.Emit;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;
using Yaplex.Azure.Storage.Exceptions;

namespace Yaplex.Azure.Storage
{
    public abstract class Database<TDatabase> where TDatabase : Database<TDatabase>, new()
    {
        private static Action<Database<TDatabase>> _tableConstructor;
        private CloudTableClient TableClient { get; set; }

        public static TDatabase Create(string connectionString)
        {
            var db = new TDatabase();
            CloudStorageAccount storageAccount = CloudStorageAccount.Parse(connectionString);
            var tableClient = storageAccount.CreateCloudTableClient();
            db.Initialize(tableClient);
            return db;
        }

        private void Initialize(CloudTableClient tableClient)
        {
            TableClient = tableClient;
            if (_tableConstructor == null)
            {
                _tableConstructor = CreateTableConstructor();
            }

            _tableConstructor(this);
        }

        private bool TableExists(string name)
        {
            return this.TableClient.GetTableReference(name).Exists();
        }

        private Action<Database<TDatabase>> CreateTableConstructor()
        {
            var dm = new DynamicMethod("ConstructInstances", null, new[] {typeof (Database<TDatabase>)}, true);
            var il = dm.GetILGenerator();

            var setters = GetType().GetProperties()
                .Where(
                    p => p.PropertyType.IsGenericType && p.PropertyType.GetGenericTypeDefinition() == typeof (Table<>))
                .Select(p => Tuple.Create(
                    p.GetSetMethod(true),
                    p.PropertyType.GetConstructor(new[] {typeof (Database<TDatabase>), typeof (string)}),
                    p.Name,
                    p.DeclaringType
                    ));

            foreach (var setter in setters)
            {
                il.Emit(OpCodes.Ldarg_0);
                // [db]

                il.Emit(OpCodes.Ldstr, setter.Item3);
                // [db, likelyname]

                il.Emit(OpCodes.Newobj, setter.Item2);
                // [table]

                var table = il.DeclareLocal(setter.Item2.DeclaringType);
                il.Emit(OpCodes.Stloc, table);
                // []

                il.Emit(OpCodes.Ldarg_0);
                // [db]

                il.Emit(OpCodes.Castclass, setter.Item4);
                // [db cast to container]

                il.Emit(OpCodes.Ldloc, table);
                // [db cast to container, table]

                il.Emit(OpCodes.Callvirt, setter.Item1);
                // []
            }

            il.Emit(OpCodes.Ret);
            return (Action<Database<TDatabase>>) dm.CreateDelegate(typeof (Action<Database<TDatabase>>));
        }

        private bool CreateTable(string tableName)
        {
            return TableClient.GetTableReference(tableName).CreateIfNotExists();
        }

        public class Table<T> where T : TableEntity, new()
        {
            private readonly CloudTable _cloudTable;
            private readonly Database<TDatabase> _database;
            private readonly string _likelyTableName;

            private string _tableName;

            public CloudTable CloudTable { get { return _cloudTable; } }

            public Table(Database<TDatabase> database, string likelyTableName)
            {
                _database = database;
                _likelyTableName = likelyTableName;
                if (!_database.TableExists(likelyTableName))
                    _database.CreateTable(likelyTableName);
                _cloudTable = _database.TableClient.GetTableReference(TableName);
            }

            public string TableName
            {
                get
                {
                    _tableName = _tableName ?? _likelyTableName;
                    return _tableName;
                }
            }

            public void Insert(T data)
            {
                TableOperation insertOp = TableOperation.Insert(data);
                var tr = _cloudTable.Execute(insertOp);
                if (tr.HttpStatusCode != (int) HttpStatusCode.NoContent &&
                    tr.HttpStatusCode != (int) HttpStatusCode.Created)
                    throw new AzureInsertOperationException("Error during inserting into table. " + tr.HttpStatusCode);
            }

            public IQueryable<T> Query()
            {
                return _cloudTable.CreateQuery<T>();
            }

            public T Get(string partitionKey, string rowKey)
            {
                TableOperation retOp = TableOperation.Retrieve<T>(partitionKey, rowKey);
                TableResult tr = _cloudTable.Execute(retOp);
                if (tr.HttpStatusCode == (int)HttpStatusCode.OK) return (T)tr.Result;
                throw new AzureRetriveOperationException("Error during Get(). " + tr.HttpStatusCode);
            }

            public void Delete(T entity)
            {
                var deleteOp = TableOperation.Delete(entity);
                var tr = _cloudTable.Execute(deleteOp);
                if (tr.HttpStatusCode != (int) HttpStatusCode.NoContent)
                    throw new AzureDeleteOperationException("Can't delete entity. " + tr.HttpStatusCode);
            }


            public T First(string partitionKey)
            {
                return Query().First(x =>partitionKey.Equals(x.PartitionKey, StringComparison.OrdinalIgnoreCase));
            }

            public void Update(T entity)
            {
                var updateOp = TableOperation.Replace(entity);
                var tr = _cloudTable.Execute(updateOp);
                if (tr.HttpStatusCode != (int)HttpStatusCode.NoContent)
                    throw new AzureDeleteOperationException("Can't update entity. " + tr.HttpStatusCode);
            }

            public IEnumerable<T> All(string partition)
            {
                return Query().Where(x=>partition.Equals(x.PartitionKey, StringComparison.OrdinalIgnoreCase));
            }

            public IEnumerable<T> ExecuteQuery(TableQuery<T> query)
            {
                return _cloudTable.ExecuteQuery(query);
            }
        }
    }
}