using Suplex.Security;
using Synapse.Core;
using Synapse.Services;
using Synapse.Services.Controller.Dal;
using Synapse.Services.Enterprise.Api;
using Synapse.Services.Enterprise.Api.Dal;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.DocumentModel;
using Newtonsoft.Json;

namespace Synapse.Controller.Dal.DynamoDb
{
    public class DynamoDbDal : IControllerDal, IEnterpriseDal
    {
        public string PlanTable { get; set; }

        public string ContainerTable { get; set; }

        private readonly AmazonDynamoDBConfig _clientConfig;
        private readonly AmazonDynamoDBClient _client;
        private readonly JsonSerializerSettings _settings = new JsonSerializerSettings()
        {
            TypeNameHandling = TypeNameHandling.Objects,
            MetadataPropertyHandling = MetadataPropertyHandling.ReadAhead
        };

        public DynamoDbDal()
        {
            _clientConfig = new AmazonDynamoDBConfig();
            _client = new AmazonDynamoDBClient( _clientConfig );
        }

        public DynamoDbDal(AmazonDynamoDBClient client, AmazonDynamoDBConfig clientConfig)
        {
            _client = client;
            _clientConfig = clientConfig;
        }

        public object GetDefaultConfig()
        {
            throw new NotImplementedException();
        }

        public Dictionary<string, string> Configure(ISynapseDalConfig conifg)
        {
            throw new NotImplementedException();
        }

        public bool HasAccess(string securityContext, string planUniqueName, FileSystemRight right = FileSystemRight.Execute)
        {
            throw new NotImplementedException();
        }

        public bool HasAccess(string securityContext, string planUniqueName, AceType aceType, object right)
        {
            throw new NotImplementedException();
        }

        public void HasAccessOrException(string securityContext, string planUniqueName, FileSystemRight right = FileSystemRight.Execute)
        {
            throw new NotImplementedException();
        }

        public void HasAccessOrException(string securityContext, string planUniqueName, AceType aceType, object right)
        {
            throw new NotImplementedException();
        }

        public IEnumerable<string> GetPlanList(string filter = null, bool isRegexFilter = true)
        {
            throw new NotImplementedException();
        }

        public IEnumerable<long> GetPlanInstanceIdList(string planUniqueName)
        {
            throw new NotImplementedException();
        }

        public Plan GetPlan(string planUniqueName)
        {
            throw new NotImplementedException();
        }

        public Plan CreatePlanInstance(string planUniqueName)
        {
            throw new NotImplementedException();
        }

        public Plan GetPlanStatus(string planUniqueName, long planInstanceId)
        {
            throw new NotImplementedException();
        }

        public void UpdatePlanStatus(Plan plan)
        {
            throw new NotImplementedException();
        }

        public void UpdatePlanStatus(PlanUpdateItem item)
        {
            throw new NotImplementedException();
        }

        public void UpdatePlanActionStatus(string planUniqueName, long planInstanceId, ActionItem actionItem)
        {
            throw new NotImplementedException();
        }

        public void UpdatePlanActionStatus(ActionUpdateItem item)
        {
            throw new NotImplementedException();
        }

        public PlanContainer GetPlanContainerByUId(Guid planContainerUId, bool autoManageSqlConnection = true, bool validateRls = false)
        {
            PlanContainer container;

            if ( planContainerUId == Guid.Empty )
                throw new Exception( "Plan container unique id cannot be empty." );

            if ( string.IsNullOrWhiteSpace( PlanTable ) )
                throw new Exception( "Plan container table name must be specified." );

            try
            {
                Table table = Table.LoadTable( _client, PlanTable );
                Document document = table.GetItem( planContainerUId );
                if ( document == null )
                    throw new Exception( "Plan container cannot be found." );

                string json = document.ToJsonPretty();
                Console.WriteLine( json );
                container = JsonConvert.DeserializeObject<PlanContainer>( json, _settings );
            }
            catch ( Exception ex )
            {
                Debug.Write( ex.Message );
                throw;
            }
            return container;
        }

        // TODO: Check with Steve on returnAsHierarchy, startUId, validateRls
        public List<PlanContainer> GetPlanContainerByAny(Guid? planContainerUId, string name, string nodeUri, string createdBy, DateTime? createdTime, string modifiedBy, DateTime? modifiedTime, bool returnAsHierarchy = false,
            Guid? startUId = null, bool validateRls = false)
        {
            List<PlanContainer> containerList = new List<PlanContainer>();

            if ( string.IsNullOrWhiteSpace( ContainerTable ) )
                throw new Exception( "Plan table name must be specified." );

            try
            {
                Table table = Table.LoadTable( _client, ContainerTable );
                if ( table != null )
                {
                    ScanFilter scanFilter = new ScanFilter();

                    if ( planContainerUId != null && planContainerUId != Guid.Empty )
                        scanFilter.AddCondition( "UId", ScanOperator.Equal, planContainerUId );
                    if ( !string.IsNullOrWhiteSpace( name ) )
                        scanFilter.AddCondition( "Name", ScanOperator.Equal, name );

                    if ( !string.IsNullOrWhiteSpace( nodeUri ) )
                        scanFilter.AddCondition( "NodeUri", ScanOperator.Equal, nodeUri );
                    if ( !string.IsNullOrWhiteSpace( createdBy ) )
                        scanFilter.AddCondition( "AuditCreatedBy", ScanOperator.Equal, createdBy );
                    if ( createdTime != null )
                        scanFilter.AddCondition( "AuditCreatedTime", ScanOperator.Equal, createdTime );
                    if ( !string.IsNullOrWhiteSpace( modifiedBy ) )
                        scanFilter.AddCondition( "AuditModifiedBy", ScanOperator.Equal, modifiedBy );
                    if ( modifiedTime != null )
                        scanFilter.AddCondition( "AuditModifiedTime", ScanOperator.Equal, modifiedTime );

                    if ( scanFilter.ToConditions().Count == 0 )
                        throw new Exception( "At least one filter condition must be specified." );

                    Search search = table.Scan( scanFilter );

                    do
                    {
                        List<Document> documentList = search.GetNextSet();
                        if ( documentList.Count == 0 )
                            throw new Exception( "Plan cannot be found." );

                        foreach ( Document document in documentList )
                        {
                            string json = document.ToJsonPretty();
                            PlanContainer container = JsonConvert.DeserializeObject<PlanContainer>( json, _settings );
                            containerList.Add( container );
                        }
                    } while ( !search.IsDone );
                }
                else
                {
                    throw new Exception( $"Dynamo table {ContainerTable} cannot be found." );
                }
            }
            catch ( Exception ex )
            {
                Debug.Write( ex.Message );
                throw;
            }

            return containerList;
        }

        public PlanContainer UpsertPlanContainer(PlanContainer planContainer)
        {
            if ( planContainer == null )
                throw new Exception( "Plan container cannot be null." );

            if ( planContainer.UId == Guid.Empty )
                throw new Exception( "Plan container unique id cannot be empty." );

            if ( string.IsNullOrWhiteSpace( planContainer.Name ) )
                throw new Exception( "Plan container name cannot be null or empty." );

            if ( string.IsNullOrWhiteSpace( PlanTable ) )
                throw new Exception( "Plan container table name must be specified." );

            try
            {
                string output = JsonConvert.SerializeObject( planContainer, Formatting.Indented, _settings );
                Document doc = Document.FromJson( output );

                Table table = Table.LoadTable( _client, PlanTable );
                table.PutItem( doc );
            }
            catch ( Exception ex )
            {
                Debug.Write( ex.Message );
                throw;
            }
            return planContainer;
        }

        public void DeletePlanContainer(Guid planUId)
        {
            if ( planUId == null || planUId == Guid.Empty )
                throw new Exception( "Plan container unique id cannot be empty." );

            if ( string.IsNullOrWhiteSpace( ContainerTable ) )
                throw new Exception( "Plan container table name must be specified." );

            try
            {
                Table table = Table.LoadTable( _client, ContainerTable );
                if ( table != null )
                {
                    table.DeleteItem( planUId );
                    Document residual = table.GetItem( planUId, new GetItemOperationConfig()
                    {
                        ConsistentRead = true
                    } );
                    if ( residual != null )
                        throw new Exception( "Plan container was not deleted successfully." );
                }
                else
                {
                    throw new Exception( $"Table {ContainerTable} cannot be found." );
                }
            }
            catch ( Exception ex )
            {
                Debug.Write( ex.Message );
                throw;
            }
        }

        public PlanItem GetPlanByUId(Guid planUId, bool autoManageSqlConnection = true, bool validateRls = false)
        {
            PlanItem plan;

            if ( planUId == Guid.Empty )
                throw new Exception( "Plan unique id cannot be empty." );

            if ( string.IsNullOrWhiteSpace( PlanTable ) )
                throw new Exception( "Plan table name must be specified." );

            try
            {
                Table table = Table.LoadTable( _client, PlanTable );
                Document document = table.GetItem( planUId );
                if ( document == null )
                    throw new Exception( "Plan cannot be found." );

                string json = document.ToJsonPretty();
                Console.WriteLine( json );
                plan = JsonConvert.DeserializeObject<PlanItem>( json, _settings );
            }
            catch ( Exception ex )
            {
                Debug.Write( ex.Message );
                throw;
            }
            return plan;
        }

        public List<PlanItem> GetPlanByAny(Guid? planUId, string name, string uniqueName, string planFile, bool? planFileIsUri, Guid? planContainerUId, string createdBy, DateTime? createdTime, string modifiedBy,
            DateTime? modifiedTime)
        {
            List<PlanItem> planList = new List<PlanItem>();

            if ( string.IsNullOrWhiteSpace( PlanTable ) )
                throw new Exception( "Plan table name must be specified." );

            try
            {
                Table table = Table.LoadTable( _client, PlanTable );
                if ( table != null )
                {
                    ScanFilter scanFilter = new ScanFilter();

                    if ( planUId != null && planUId != Guid.Empty )
                        scanFilter.AddCondition( "UId", ScanOperator.Equal, planUId );
                    if ( !string.IsNullOrWhiteSpace( name ) )
                        scanFilter.AddCondition( "Name", ScanOperator.Equal, name );
                    if ( !string.IsNullOrWhiteSpace( uniqueName ) )
                        scanFilter.AddCondition( "UniqueName", ScanOperator.Equal, uniqueName );
                    if ( !string.IsNullOrWhiteSpace( planFile ) )
                        scanFilter.AddCondition( "PlanFile", ScanOperator.Equal, planFile );
                    if ( planFileIsUri != null )
                        scanFilter.AddCondition( "PlanFileIsUri", ScanOperator.Equal, planFileIsUri );
                    if ( planContainerUId != null && planContainerUId != Guid.Empty )
                        scanFilter.AddCondition( "PlanContainerUId", ScanOperator.Equal, planContainerUId );
                    if ( !string.IsNullOrWhiteSpace( createdBy ) )
                        scanFilter.AddCondition( "AuditCreatedBy", ScanOperator.Equal, createdBy );
                    if ( createdTime != null )
                        scanFilter.AddCondition( "AuditCreatedTime", ScanOperator.Equal, createdTime );
                    if ( !string.IsNullOrWhiteSpace( modifiedBy ) )
                        scanFilter.AddCondition( "AuditModifiedBy", ScanOperator.Equal, modifiedBy );
                    if ( modifiedTime != null )
                        scanFilter.AddCondition( "AuditModifiedTime", ScanOperator.Equal, modifiedTime );

                    if ( scanFilter.ToConditions().Count == 0 )
                        throw new Exception( "At least one filter condition must be specified." );

                    Search search = table.Scan( scanFilter );

                    do
                    {
                        List<Document> documentList = search.GetNextSet();
                        if ( documentList.Count == 0 )
                            throw new Exception( "Plan cannot be found." );

                        foreach ( Document document in documentList )
                        {
                            string json = document.ToJsonPretty();
                            PlanItem plan = JsonConvert.DeserializeObject<PlanItem>( json, _settings );
                            planList.Add( plan );
                        }
                    } while ( !search.IsDone );
                }
                else
                {
                    throw new Exception( $"Dynamo table {PlanTable} cannot be found." );
                }
            }
            catch ( Exception ex )
            {
                Debug.Write( ex.Message );
                throw;
            }

            return planList;
        }

        public PlanItem UpsertPlan(PlanItem plan)
        {
            if ( plan == null )
                throw new Exception( "Plan cannot be null." );

            if ( plan.UId == Guid.Empty )
                throw new Exception( "Plan unique id cannot be empty." );

            if ( string.IsNullOrWhiteSpace( plan.Name ) )
                throw new Exception( "Plan name cannot be null or empty." );

            if ( string.IsNullOrWhiteSpace( plan.UniqueName ) )
                throw new Exception( "Plan unique name cannot be null or empty." );

            if ( string.IsNullOrWhiteSpace( PlanTable ) )
                throw new Exception( "Plan table name must be specified." );

            try
            {
                string output = JsonConvert.SerializeObject( plan, Formatting.Indented, _settings );
                Document doc = Document.FromJson( output );

                Table table = Table.LoadTable( _client, PlanTable );
                table.PutItem( doc );
            }
            catch ( Exception ex )
            {
                Debug.Write( ex.Message );
                throw;
            }
            return plan;
        }

        public void DeletePlan(Guid planUId)
        {
            if ( planUId == null || planUId == Guid.Empty )
                throw new Exception( "Plan unique id cannot be empty." );

            if ( string.IsNullOrWhiteSpace( PlanTable ) )
                throw new Exception( "Plan table name must be specified." );

            try
            {
                Table table = Table.LoadTable( _client, PlanTable );
                if ( table != null )
                {
                    table.DeleteItem( planUId );
                    Document residual = table.GetItem( planUId, new GetItemOperationConfig()
                    {
                        ConsistentRead = true
                    } );
                    if ( residual != null )
                        throw new Exception( "Plan was not deleted successfully." );
                }
                else
                {
                    throw new Exception( $"Table {PlanTable} cannot be found." );
                }
            }
            catch ( Exception ex )
            {
                Debug.Write( ex.Message );
                throw;
            }
        }
    }
}
