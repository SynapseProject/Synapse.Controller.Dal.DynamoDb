using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Amazon.DynamoDBv2.Model;
using NUnit.Framework;
using Synapse.Services.Enterprise.Api;

namespace Synapse.Controller.Dal.DynamoDb.Tests
{
    public class DynamoDbTests
    {
        private string _planTable = "Synapse.PlanItem";
        private string _planPrefix = "Plan.";
        private string _containerTable = "Synapse.PlanContainer";
        private string _containerPrefix = "Container.";

        [Test]
        public void DeletePlan_Empty_PlanUId_Throws_Exception()
        {
            // Arrange
            Guid planUId = Guid.Empty;
            DynamoDbDal dal = new DynamoDbDal
            {
                PlanTable = _planTable
            };

            // Act
            Exception ex = Assert.Throws<Exception>( () => dal.DeletePlan( planUId ) );

            // Assert
            StringAssert.AreEqualIgnoringCase( "Plan unique id cannot be empty.", ex.Message );
        }

        [Test]
        public void DeletePlan_Null_Plan_Table_Throws_Exception()
        {
            // Arrange
            Guid planUId = Guid.NewGuid();
            DynamoDbDal dal = new DynamoDbDal
            {
                PlanTable = ""
            };

            // Act
            Exception ex = Assert.Throws<Exception>( () => dal.DeletePlan( planUId ) );

            // Assert
            StringAssert.AreEqualIgnoringCase( ex.Message, "Plan table name must be specified." );
        }

        [Test]
        public void DeletePlan_Non_Existent_Table_Throws_Exception()
        {
            // Arrange
            Guid planUId = Guid.NewGuid();
            DynamoDbDal dal = new DynamoDbDal
            {
                PlanTable = "XXXXXX"
            };

            // Act
            Exception ex = Assert.Throws<ResourceNotFoundException>( () => dal.DeletePlan( planUId ) );

            // Assert
            StringAssert.Contains( "Requested resource not found: Table", ex.Message );
        }

        [Test]
        public void DeletePlan_Non_Existent_Plan_Succeeds()
        {
            // Arrange
            Guid planUId = Guid.NewGuid();
            DynamoDbDal dal = new DynamoDbDal
            {
                PlanTable = _planTable
            };

            // Act
            // Assert
            Assert.DoesNotThrow( () => dal.DeletePlan( planUId ) );
        }

        [Test]
        public void DeletePlan_Existing_Plan_Succeeds()
        {
            // Arrange
            Guid planUId = Guid.NewGuid();
            PlanItem plan = new PlanItem()
            {
                UId = planUId,
                Name = _planPrefix + planUId,
                UniqueName = _planPrefix + planUId
            };
            DynamoDbDal dal = new DynamoDbDal
            {
                PlanTable = _planTable
            };

            // Act
            dal.UpsertPlan( plan );

            Assert.DoesNotThrow( () => dal.DeletePlan( planUId ) );
        }


        [Test]
        public void DeletePlanContainer_Empty_UId_Throws_Exception()
        {
            // Arrange
            Guid containerUId = Guid.Empty;
            DynamoDbDal dal = new DynamoDbDal
            {
                ContainerTable = _containerTable
            };

            // Act
            Exception ex = Assert.Throws<Exception>( () => dal.DeletePlanContainer( containerUId ) );

            // Assert
            StringAssert.AreEqualIgnoringCase( "Plan container unique id cannot be empty.", ex.Message );
        }

        [Test]
        public void DeletePlanContainer_Null_Container_Table_Throws_Exception()
        {
            // Arrange
            Guid containerUId = Guid.NewGuid();
            DynamoDbDal dal = new DynamoDbDal
            {
                ContainerTable = ""
            };

            // Act
            Exception ex = Assert.Throws<Exception>( () => dal.DeletePlanContainer( containerUId ) );

            // Assert
            StringAssert.AreEqualIgnoringCase( ex.Message, "Plan container table name must be specified." );
        }

        [Test]
        public void DeletePlanContainer_Non_Existent_Table_Throws_Exception()
        {
            // Arrange
            Guid containerUId = Guid.NewGuid();
            DynamoDbDal dal = new DynamoDbDal
            {
                ContainerTable = "XXXXXX"
            };

            // Act
            Exception ex = Assert.Throws<ResourceNotFoundException>( () => dal.DeletePlanContainer( containerUId ) );

            // Assert
            StringAssert.Contains( "Requested resource not found: Table", ex.Message );
        }

        [Test]
        public void DeletePlanContainer_Non_Existent_Container_Succeeds()
        {
            // Arrange
            Guid containerUId = Guid.NewGuid();
            DynamoDbDal dal = new DynamoDbDal
            {
                ContainerTable = _containerTable
            };

            // Act
            // Assert
            Assert.DoesNotThrow( () => dal.DeletePlanContainer( containerUId ) );
        }

        [Test]
        public void DeletePlanContainer_Existing_Container_Succeeds()
        {
            // Arrange
            Guid containerUId = Guid.NewGuid();
            ;
            PlanContainer container = new PlanContainer()
            {
                UId = containerUId,
                Name = _containerPrefix + containerUId
            };
            DynamoDbDal dal = new DynamoDbDal
            {
                ContainerTable = _containerTable
            };

            // Act
            dal.UpsertPlanContainer( container );

            Assert.DoesNotThrow( () => dal.DeletePlanContainer( containerUId ) );
        }


        [Test]
        public void GetPlanByAny_No_Filter_Condition_Throws_Exception()
        {
            // Arrange
            DynamoDbDal dal = new DynamoDbDal
            {
                PlanTable = _planTable
            };

            // Act
            Exception ex = Assert.Throws<Exception>( () => dal.GetPlanByAny( null, null, null, null, null, null, null, null, null, null ) );

            // Assert
            StringAssert.AreEqualIgnoringCase( "At least one filter condition must be specified.", ex.Message );
        }

        [Test]
        public void GetPlanByAny_Null_Plan_Table_Throws_Exception()
        {
            // Arrange
            Guid planUId = Guid.Empty;

            DynamoDbDal dal = new DynamoDbDal
            {
                PlanTable = ""
            };

            // Act
            Exception ex = Assert.Throws<Exception>( () => dal.GetPlanByAny( planUId, null, null, null, null, null, null, null, null, null ) );

            // Assert
            StringAssert.AreEqualIgnoringCase( "Plan table name must be specified.", ex.Message );
        }

        [Test]
        public void GetPlanByAny_Non_Existent_Plan_Throws_Exception()
        {
            // Arrange
            Guid planUId = Guid.NewGuid();

            DynamoDbDal dal = new DynamoDbDal
            {
                PlanTable = _planTable
            };

            // Act
            Exception ex = Assert.Throws<Exception>( () => dal.GetPlanByAny( planUId, null, null, null, null, null, null, null, null, null ) );

            // Assert
            StringAssert.AreEqualIgnoringCase( "Plan cannot be found.", ex.Message );
        }

        [Test]
        public void GetPlanByAny_Non_Existent_Table_Throws_Exception()
        {
            // Arrange
            Guid planUId = Guid.NewGuid();

            DynamoDbDal dal = new DynamoDbDal
            {
                PlanTable = "XXXXXX"
            };

            // Act
            // Assert
            ResourceNotFoundException ex = Assert.Throws<ResourceNotFoundException>( () => dal.GetPlanByAny( planUId, null, null, null, null, null, null, null, null, null ) );
            StringAssert.Contains( "Requested resource not found: Table", ex.Message );
        }

        [Test]
        public void GetPlanByAny_Existing_Plan_Succeeds()
        {
            // Arrange
            Guid planUId = Guid.NewGuid();
            PlanItem plan = new PlanItem()
            {
                UId = planUId,
                Name = _planPrefix + planUId,
                UniqueName = _planPrefix + planUId,
                PlanFileIsUri = false,
                IsActive = false
            };
            DynamoDbDal dal = new DynamoDbDal
            {
                PlanTable = _planTable
            };

            // Act
            dal.UpsertPlan( plan );
            var retPlans = dal.GetPlanByAny( plan.UId, null, null, null, null, null, null, null, null, null );

            // Assert
            Assert.IsTrue( retPlans.Count > 0 );
            foreach ( PlanItem p in retPlans )
            {
                Assert.AreEqual( p.UId, plan.UId );
                Assert.AreEqual( p.Name, plan.Name );
                Assert.AreEqual( p.IsActive, plan.IsActive );
                Assert.AreEqual( p.PlanFileIsUri, plan.PlanFileIsUri );
            }
        }

        [Test]
        public void GetPlanByUId_Empty_PlanUId_Throws_Exception()
        {
            // Arrange
            Guid planUId = Guid.Empty;

            DynamoDbDal dal = new DynamoDbDal
            {
                PlanTable = _planTable
            };

            // Act
            Exception ex = Assert.Throws<Exception>( () => dal.GetPlanByUId( planUId ) );

            // Assert
            StringAssert.AreEqualIgnoringCase( "Plan unique id cannot be empty.", ex.Message );
        }

        [Test]
        public void GetPlanByUId_Null_Plan_Table_Throws_Exception()
        {
            // Arrange
            Guid planUId = Guid.NewGuid();

            DynamoDbDal dal = new DynamoDbDal
            {
                PlanTable = ""
            };
            // Act
            Exception ex = Assert.Throws<Exception>( () => dal.GetPlanByUId( planUId ) );

            // Assert
            StringAssert.AreEqualIgnoringCase( ex.Message, "Plan table name must be specified." );
        }

        [Test]
        public void GetPlanByUId_Non_Existent_Table_Throws_Exception()
        {
            // Arrange
            Guid planUId = Guid.NewGuid();

            DynamoDbDal dal = new DynamoDbDal
            {
                PlanTable = "XXXXXX"
            };

            // Act
            Exception ex = Assert.Throws<ResourceNotFoundException>( () => dal.GetPlanByUId( planUId ) );

            // Assert
            StringAssert.Contains( "Requested resource not found: Table", ex.Message );
        }

        [Test]
        public void GetPlanByUId_Non_Existent_Plan_Throws_Exception()
        {
            // Arrange
            Guid planUId = Guid.NewGuid();


            DynamoDbDal dal = new DynamoDbDal
            {
                PlanTable = _planTable
            };

            // Act
            Exception ex = Assert.Throws<Exception>( () => dal.GetPlanByUId( planUId ) );

            // Assert
            StringAssert.AreEqualIgnoringCase( "Plan cannot be found.", ex.Message );
        }

        [Test]
        public void GetPlanByUId_Existing_Plan_Succeeds()
        {
            // Arrange
            Guid planUId = Guid.NewGuid();
            PlanItem plan = new PlanItem()
            {
                UId = planUId,
                Name = _planPrefix + planUId,
                UniqueName = _planPrefix + planUId,
                IsActive = false
            };
            DynamoDbDal dal = new DynamoDbDal
            {
                PlanTable = _planTable
            };

            // Act
            dal.UpsertPlan( plan );
            PlanItem retPlan = dal.GetPlanByUId( plan.UId );

            // Assert
            Assert.AreEqual( retPlan.UId, plan.UId );
            Assert.AreEqual( retPlan.Name, plan.Name );
            Assert.AreEqual( retPlan.IsActive, plan.IsActive );
        }

        [Test]
        public void GetPlanContainerByAny_No_Filter_Condition_Throws_Exception()
        {
            // Arrange
            DynamoDbDal dal = new DynamoDbDal
            {
                ContainerTable = _containerTable
            };

            // Act
            Exception ex = Assert.Throws<Exception>( () => dal.GetPlanContainerByAny( null, null, null, null, null, null, null ) );

            // Assert
            StringAssert.AreEqualIgnoringCase( "At least one filter condition must be specified.", ex.Message );
        }

        [Test]
        public void GetPlanContainerByAny_Null_Plan_Container_Table_Throws_Exception()
        {
            // Arrange
            Guid containerUId = Guid.NewGuid();

            DynamoDbDal dal = new DynamoDbDal
            {
                ContainerTable = ""
            };

            // Act
            Exception ex = Assert.Throws<Exception>( () => dal.GetPlanContainerByAny( containerUId, null, null, null, null, null, null ) );

            // Assert
            StringAssert.AreEqualIgnoringCase( "Plan container table name must be specified.", ex.Message );
        }

        [Test]
        public void GetPlanContainerByAny_Non_Existent_Plan_Container_Throws_Exception()
        {
            // Arrange
            Guid containerUId = Guid.NewGuid();

            DynamoDbDal dal = new DynamoDbDal
            {
                ContainerTable = _containerTable
            };

            // Act
            Exception ex = Assert.Throws<Exception>( () => dal.GetPlanContainerByAny( containerUId, null, null, null, null, null, null ) );

            // Assert
            StringAssert.AreEqualIgnoringCase( "Plan container cannot be found.", ex.Message );
        }

        [Test]
        public void GetPlanContainerByAny_Non_Existent_Table_Throws_Exception()
        {
            // Arrange
            Guid containerUId = Guid.NewGuid();

            DynamoDbDal dal = new DynamoDbDal
            {
                ContainerTable = "XXXXXX"
            };

            // Act
            // Assert
            ResourceNotFoundException ex = Assert.Throws<ResourceNotFoundException>( () => dal.GetPlanContainerByAny( containerUId, null, null, null, null, null, null ) );
            StringAssert.Contains( "Requested resource not found: Table", ex.Message );
        }


        [Test]
        public void GetPlanContainerByAny_Existing_Plan_Container_Succeeds()
        {
            Guid containerUId = Guid.NewGuid();
            PlanContainer container = new PlanContainer()
            {
                UId = containerUId,
                Name = _containerPrefix + containerUId
            };
            DynamoDbDal dal = new DynamoDbDal
            {
                ContainerTable = _containerTable
            };

            // Act
            dal.UpsertPlanContainer( container );
            var retContainers = dal.GetPlanContainerByAny( containerUId, null, null, null, null, null, null );

            // Assert
            Assert.IsTrue( retContainers.Count > 0 );
            foreach ( PlanContainer c in retContainers )
            {
                Assert.AreEqual( c.UId, container.UId );
                Assert.AreEqual( c.Name, container.Name );
            }

        }

        [Test]
        public void GetPlanContainerByUId_Empty_Plan_Container_UId_Throws_Exception()
        {
            // Arrange
            Guid containerUId = Guid.Empty;

            DynamoDbDal dal = new DynamoDbDal
            {
                ContainerTable = _containerTable
            };

            // Act
            Exception ex = Assert.Throws<Exception>( () => dal.GetPlanContainerByUId( containerUId ) );

            // Assert
            StringAssert.AreEqualIgnoringCase( "Plan container unique id cannot be empty.", ex.Message );
        }

        [Test]
        public void GetPlanContainerByUId_Null_Plan_Container_Table_Throws_Exception()
        {
            // Arrange
            Guid containerUId = Guid.NewGuid();

            DynamoDbDal dal = new DynamoDbDal
            {
                ContainerTable = ""
            };

            // Act
            Exception ex = Assert.Throws<Exception>( () => dal.GetPlanContainerByUId( containerUId ) );

            // Assert
            StringAssert.AreEqualIgnoringCase( "Plan container table name must be specified.", ex.Message );
        }

        [Test]
        public void GetPlanContainerByUId_Non_Existent_Table_Throws_Exception()
        {
            // Arrange
            Guid containerUId = Guid.NewGuid();

            DynamoDbDal dal = new DynamoDbDal
            {
                ContainerTable = "XXXXXX"
            };

            // Act
            Exception ex = Assert.Throws<ResourceNotFoundException>( () => dal.GetPlanContainerByUId( containerUId ) );

            // Assert
            StringAssert.Contains( "Requested resource not found: Table", ex.Message );
        }

        [Test]
        public void GetPlanContainerByUId_Non_Existent_Plan_Container_Throws_Exception()
        {
            // Arrange
            Guid containerUId = Guid.NewGuid();

            DynamoDbDal dal = new DynamoDbDal
            {
                ContainerTable = _containerTable
            };

            // Act
            Exception ex = Assert.Throws<Exception>( () => dal.GetPlanContainerByUId( containerUId ) );

            // Assert
            StringAssert.Contains( "Plan container cannot be found.", ex.Message );
        }

        [Test]
        public void GetPlanContainerByUId_Existing_Plan_Container_Succeeds()
        {
            // Arrange
            Guid containerUId = Guid.NewGuid();
            ;
            PlanContainer container = new PlanContainer()
            {
                UId = containerUId,
                Name = _containerPrefix + containerUId
            };
            DynamoDbDal dal = new DynamoDbDal
            {
                ContainerTable = _containerTable
            };

            // Act
            dal.UpsertPlanContainer( container );
            PlanContainer retContainer = dal.GetPlanContainerByUId( container.UId );

            // Assert
            Assert.AreEqual( container.UId, retContainer.UId );
            Assert.AreEqual( container.Name, retContainer.Name );
            Assert.AreEqual( container.Description, retContainer.Description );
        }

        [Test]
        public void UpsertPlan_Null_Plan_Throws_Exception()
        {
            // Arrange
            PlanItem planItem = null;
            DynamoDbDal dal = new DynamoDbDal
            {
                PlanTable = _planTable
            };

            // Act
            Exception ex = Assert.Throws<Exception>( () => dal.UpsertPlan( planItem ) );

            // Assert
            StringAssert.AreEqualIgnoringCase( "Plan cannot be null.", ex.Message );
        }

        [Test]
        public void UpsertPlan_Null_Plan_Table_Throws_Exception()
        {
            // Arrange
            Guid planUId = Guid.NewGuid();
            PlanItem plan = new PlanItem()
            {
                UId = planUId,
                Name = _planPrefix + planUId,
                UniqueName = _planPrefix + planUId
            };
            DynamoDbDal dal = new DynamoDbDal
            {
                PlanTable = ""
            };

            // Act
            Exception ex = Assert.Throws<Exception>( () => dal.UpsertPlan( plan ) );

            // Assert
            StringAssert.AreEqualIgnoringCase( "Plan table name must be specified.", ex.Message );
        }

        [Test]
        public void UpsertPlan_Valid_Details_Succeeds()
        {
            // Arrange
            Guid planUId = Guid.NewGuid();
            PlanItem plan = new PlanItem()
            {
                UId = planUId,
                Name = _planPrefix + planUId,
                UniqueName = _planPrefix + planUId
            };
            DynamoDbDal dal = new DynamoDbDal
            {
                PlanTable = _planTable
            };

            // Act
            dal.UpsertPlan( plan );
            PlanItem retUser = dal.GetPlanByUId( plan.UId );

            // Assert
            Assert.AreEqual( plan.UId, retUser.UId );
            Assert.AreEqual( plan.Name, retUser.Name );
            Assert.AreEqual( plan.UniqueName, retUser.UniqueName );
        }

        [Test]
        public void UpsertPlan_Non_Existent_Table_Throws_Exception()
        {
            // Arrange
            Guid planUId = Guid.NewGuid();
            PlanItem plan = new PlanItem()
            {
                UId = planUId,
                Name = _planPrefix + planUId,
                UniqueName = _planPrefix + planUId
            };
            DynamoDbDal dal = new DynamoDbDal
            {
                PlanTable = "XXXXXX"
            };

            // Act
            ResourceNotFoundException ex = Assert.Throws<ResourceNotFoundException>( () => dal.UpsertPlan( plan ) );
            StringAssert.Contains( "Requested resource not found: Table", ex.Message );
        }

        [Test]
        public void UpsertPlan_Existing_User_Succeeds()
        {
            // Arrange
            Guid planUId = Guid.NewGuid();
            PlanItem plan = new PlanItem()
            {
                UId = planUId,
                Name = _planPrefix + planUId,
                UniqueName = _planPrefix + planUId,
                IsActive = false
            };
            DynamoDbDal dal = new DynamoDbDal
            {
                PlanTable = _planTable
            };

            // Act
            dal.UpsertPlan( plan );
            plan.IsActive = true;
            dal.UpsertPlan( plan );
            PlanItem retPlan = dal.GetPlanByUId( plan.UId );

            // Assert
            Assert.AreEqual( retPlan.UId, plan.UId );
            Assert.AreEqual( retPlan.Name, plan.Name );
            Assert.AreEqual( retPlan.IsActive, plan.IsActive );
        }

        [Test]
        public void UpsertPlanContainer_Null_Plan_Container_Throws_Exception()
        {
            // Arrange
            PlanContainer container = null;
            DynamoDbDal dal = new DynamoDbDal
            {
                ContainerTable = _containerTable
            };

            // Act
            Exception ex = Assert.Throws<Exception>( () => dal.UpsertPlanContainer( container ) );

            // Assert
            StringAssert.AreEqualIgnoringCase( "Plan container cannot be null.", ex.Message );
        }

        [Test]
        public void UpsertPlanContainer_Empty_Plan_Container_UId_Throws_Exception()
        {
            // Arrange
            Guid containerUId = Guid.Empty;
            ;
            PlanContainer container = new PlanContainer()
            {
                UId = containerUId
            };
            DynamoDbDal dal = new DynamoDbDal
            {
                ContainerTable = _containerTable
            };

            // Act
            Exception ex = Assert.Throws<Exception>( () => dal.UpsertPlanContainer( container ) );

            // Assert
            StringAssert.AreEqualIgnoringCase( "Plan container unique id cannot be empty.", ex.Message );
        }

        [Test]
        public void UpsertPlanContainer_Plan_Container_Without_Name_Throws_Exception()
        {
            // Arrange
            Guid containerUId = Guid.NewGuid();
            ;
            PlanContainer container = new PlanContainer()
            {
                UId = containerUId
            };
            DynamoDbDal dal = new DynamoDbDal
            {
                ContainerTable = _containerTable
            };

            // Act
            Exception ex = Assert.Throws<Exception>( () => dal.UpsertPlanContainer( container ) );

            // Assert
            StringAssert.AreEqualIgnoringCase( "Plan container name cannot be null or empty.", ex.Message );
        }

        [Test]
        public void UpsertPlanContainer_Null_Plan_Container_Table_Throws_Exception()
        {
            // Arrange
            Guid containerUId = Guid.NewGuid();
            ;
            PlanContainer container = new PlanContainer()
            {
                UId = containerUId,
                Name = _containerPrefix + containerUId
            };
            DynamoDbDal dal = new DynamoDbDal
            {
                ContainerTable = ""
            };

            // Act
            Exception ex = Assert.Throws<Exception>( () => dal.UpsertPlanContainer( container ) );

            // Assert
            StringAssert.AreEqualIgnoringCase( "Plan container table name must be specified.", ex.Message );
        }

        [Test]
        public void UpsertPlanContainer_Valid_Details_Succeeds()
        {
            // Arrange
            Guid containerUId = Guid.NewGuid();
            ;
            PlanContainer container = new PlanContainer()
            {
                UId = containerUId,
                Name = _containerPrefix + containerUId
            };
            DynamoDbDal dal = new DynamoDbDal
            {
                ContainerTable = _containerTable
            };

            // Act
            dal.UpsertPlanContainer( container );
            PlanContainer retContainer = dal.GetPlanContainerByUId( container.UId );

            // Assert
            Assert.AreEqual( container.UId, retContainer.UId );
            Assert.AreEqual( container.Name, retContainer.Name );
        }

        [Test]
        public void UpsertPlanContainer_Non_Existent_Table_Throws_Exception()
        {
            // Arrange
            Guid containerUId = Guid.NewGuid();
            ;
            PlanContainer container = new PlanContainer()
            {
                UId = containerUId,
                Name = _containerPrefix + containerUId
            };
            DynamoDbDal dal = new DynamoDbDal
            {
                ContainerTable = "XXXXXX"
            };

            // Act
            Exception ex = Assert.Throws<ResourceNotFoundException>( () => dal.UpsertPlanContainer( container ) );

            // Assert
            StringAssert.Contains( "Requested resource not found: Table", ex.Message );
        }

        [Test]
        public void UpsertPlanContainer_Existing_Plan_Container_Succeeds()
        {
            // Arrange
            Guid containerUId = Guid.NewGuid();
            ;
            PlanContainer container = new PlanContainer()
            {
                UId = containerUId,
                Name = _containerPrefix + containerUId
            };
            DynamoDbDal dal = new DynamoDbDal
            {
                ContainerTable = _containerTable
            };

            // Act
            dal.UpsertPlanContainer( container );
            container.Description = "Modified";
            dal.UpsertPlanContainer( container );
            PlanContainer retContainer = dal.GetPlanContainerByUId( container.UId );

            // Assert
            Assert.AreEqual( container.UId, retContainer.UId );
            Assert.AreEqual( container.Name, retContainer.Name );
            Assert.AreEqual( container.Description, retContainer.Description );
        }
    }
}
