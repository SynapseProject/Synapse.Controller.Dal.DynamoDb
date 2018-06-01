using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
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
        public void UpsertUser_Valid_Details_Succeeds()
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
    }
}
