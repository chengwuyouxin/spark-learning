package com.lpq.spark.core.listener

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.plans.logical.{Deduplicate, _}
import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.execution.datasources.CreateTable
import org.apache.spark.sql.util.QueryExecutionListener

import scala.collection.mutable

class MyQueryExecutionListener extends QueryExecutionListener with Logging{
  override def onSuccess(funcName: String, qe: QueryExecution, durationNs: Long): Unit = {
    logError("MyQueryExecutionListeneron.onSuccess")
    val result = resolveLogicPlan(qe.analyzed,"default");
    val input:mutable.HashSet[DcTable] = result._1
    val output:mutable.HashSet[DcTable] =result._2
    input.map(println(_))
    output.map(println(_))
  }

  override def onFailure(funcName: String, qe: QueryExecution, exception: Exception): Unit = {
    logError("MyQueryExecutionListeneron.onFailure")
  }

  def resolveLogicPlan(plan: LogicalPlan, currentDB:String): (mutable.HashSet[DcTable], mutable.HashSet[DcTable]) ={
    val inputTables = new mutable.HashSet[DcTable]()
    val outputTables = new mutable.HashSet[DcTable]()
    resolveLogic(plan, currentDB, inputTables, outputTables)
    Tuple2(inputTables, outputTables)
  }

  def resolveLogic(plan: LogicalPlan, currentDB:String, inputTables:mutable.HashSet[DcTable], outputTables:mutable.HashSet[DcTable]): Unit ={
    plan match {

      case plan: Project =>
        val project = plan.asInstanceOf[Project]
        resolveLogic(project.child, currentDB, inputTables, outputTables)

      case plan: Union =>
        val project = plan.asInstanceOf[Union]
        for(child <- project.children){
          resolveLogic(child, currentDB, inputTables, outputTables)
        }

      case plan: Join =>
        val project = plan.asInstanceOf[Join]
        resolveLogic(project.left, currentDB, inputTables, outputTables)
        resolveLogic(project.right, currentDB, inputTables, outputTables)

      case plan: Aggregate =>
        val project = plan.asInstanceOf[Aggregate]
        resolveLogic(project.child, currentDB, inputTables, outputTables)

      case plan: Filter =>
        val project = plan.asInstanceOf[Filter]
        resolveLogic(project.child, currentDB, inputTables, outputTables)

      case plan: Generate =>
        val project = plan.asInstanceOf[Generate]
        resolveLogic(project.child, currentDB, inputTables, outputTables)

      case plan: RepartitionByExpression =>
        val project = plan.asInstanceOf[RepartitionByExpression]
        resolveLogic(project.child, currentDB, inputTables, outputTables)

      case plan: SerializeFromObject =>
        val project = plan.asInstanceOf[SerializeFromObject]
        resolveLogic(project.child, currentDB, inputTables, outputTables)

      case plan: MapPartitions =>
        val project = plan.asInstanceOf[MapPartitions]
        resolveLogic(project.child, currentDB, inputTables, outputTables)

      case plan: DeserializeToObject =>
        val project = plan.asInstanceOf[DeserializeToObject]
        resolveLogic(project.child, currentDB, inputTables, outputTables)

      case plan: Repartition =>
        val project = plan.asInstanceOf[Repartition]
        resolveLogic(project.child, currentDB, inputTables, outputTables)

      case plan: Deduplicate =>
        val project = plan.asInstanceOf[Deduplicate]
        resolveLogic(project.child, currentDB, inputTables, outputTables)

      case plan: Window =>
        val project = plan.asInstanceOf[Window]
        resolveLogic(project.child, currentDB, inputTables, outputTables)

      case plan: MapElements =>
        val project = plan.asInstanceOf[MapElements]
        resolveLogic(project.child, currentDB, inputTables, outputTables)

      case plan: TypedFilter =>
        val project = plan.asInstanceOf[TypedFilter]
        resolveLogic(project.child, currentDB, inputTables, outputTables)

      case plan: Distinct =>
        val project = plan.asInstanceOf[Distinct]
        resolveLogic(project.child, currentDB, inputTables, outputTables)

      case plan: SubqueryAlias =>
        val project = plan.asInstanceOf[SubqueryAlias]
        val childInputTables = new mutable.HashSet[DcTable]()
        val childOutputTables = new mutable.HashSet[DcTable]()

        resolveLogic(project.child, currentDB, childInputTables, childOutputTables)
        if(childInputTables.size > 0){
          for(table <- childInputTables) inputTables.add(table)
        }else{
          inputTables.add(new DcTable(currentDB, project.alias))
        }

//      case plan: CatalogRelation =>
//        val project = plan.asInstanceOf[CatalogRelation]
//        val identifier = project.tableMeta.identifier
//        val dcTable = new DcTable(identifier.database.getOrElse(currentDB), identifier.table)
//        inputTables.add(dcTable)

//      case plan: UnresolvedRelation =>
//        val project = plan.asInstanceOf[UnresolvedRelation]
//        val dcTable = new DcTable(project.databaseName.getOrElse(currentDB), project.tableName)
//        inputTables.add(dcTable)

//      case plan: InsertIntoTable =>
//        val project = plan.asInstanceOf[InsertIntoTable]
//        resolveLogic(project.table, currentDB, outputTables, inputTables)
//        resolveLogic(project.child, currentDB, inputTables, outputTables)

      case plan: CreateTable =>
        val project = plan.asInstanceOf[CreateTable]
        if(project.query.isDefined){
          resolveLogic(project.query.get, currentDB, inputTables, outputTables)
        }
        val tableIdentifier = project.tableDesc.identifier
        val dcTable = new DcTable(tableIdentifier.database.getOrElse(currentDB), tableIdentifier.table)
        outputTables.add(dcTable)

      case plan: GlobalLimit =>
        val project = plan.asInstanceOf[GlobalLimit]
        resolveLogic(project.child, currentDB, inputTables, outputTables)

      case plan: LocalLimit =>
        val project = plan.asInstanceOf[LocalLimit]
        resolveLogic(project.child, currentDB, inputTables, outputTables)

      case `plan` => logInfo("******child plan******:\n"+plan)
    }
  }

//  def resolveLogicPlan(plan: LogicalPlan, currentDB:String): (mutable.HashSet[DcTable], mutable.HashSet[DcTable]) ={
//    val inputTables = new mutable.HashSet[DcTable]()
//    val outputTables = new mutable.HashSet[DcTable]()
//    resolveLogic(plan, currentDB, inputTables, outputTables)
//    Tuple2(inputTables, outputTables)
//  }
//
//  def resolveLogic(plan: LogicalPlan, currentDB:String, inputTables:mutable.HashSet[DcTable], outputTables:mutable.HashSet[DcTable]): Unit = {
//    plan match {
//
//      case plan: Project =>
//        val project = plan.asInstanceOf[Project]
//        resolveLogic(project.child, currentDB, inputTables, outputTables)
//
//      case plan: Union =>
//        val project = plan.asInstanceOf[Union]
//        for (child <- project.children) {
//          resolveLogic(child, currentDB, inputTables, outputTables)
//        }
//
//      case plan: Join =>
//        val project = plan.asInstanceOf[Join]
//        resolveLogic(project.left, currentDB, inputTables, outputTables)
//        resolveLogic(project.right, currentDB, inputTables, outputTables)
//
//      case plan: Aggregate =>
//        val project = plan.asInstanceOf[Aggregate]
//        resolveLogic(project.child, currentDB, inputTables, outputTables)
//
//      case plan: Filter =>
//        val project = plan.asInstanceOf[Filter]
//        resolveLogic(project.child, currentDB, inputTables, outputTables)
//
//      case plan: Generate =>
//        val project = plan.asInstanceOf[Generate]
//        resolveLogic(project.child, currentDB, inputTables, outputTables)
//
//      case plan: RepartitionByExpression =>
//        val project = plan.asInstanceOf[RepartitionByExpression]
//        resolveLogic(project.child, currentDB, inputTables, outputTables)
//
//      case plan: SerializeFromObject =>
//        val project = plan.asInstanceOf[SerializeFromObject]
//        resolveLogic(project.child, currentDB, inputTables, outputTables)
//
//      case plan: MapPartitions =>
//        val project = plan.asInstanceOf[MapPartitions]
//        resolveLogic(project.child, currentDB, inputTables, outputTables)
//
//      case plan: DeserializeToObject =>
//        val project = plan.asInstanceOf[DeserializeToObject]
//        resolveLogic(project.child, currentDB, inputTables, outputTables)
//
//      case plan: Repartition =>
//        val project = plan.asInstanceOf[Repartition]
//        resolveLogic(project.child, currentDB, inputTables, outputTables)
//
//      case plan: Deduplicate =>
//        val project = plan.asInstanceOf[Deduplicate]
//        resolveLogic(project.child, currentDB, inputTables, outputTables)
//
//      case plan: Window =>
//        val project = plan.asInstanceOf[Window]
//        resolveLogic(project.child, currentDB, inputTables, outputTables)
//
//      case plan: MapElements =>
//        val project = plan.asInstanceOf[MapElements]
//        resolveLogic(project.child, currentDB, inputTables, outputTables)
//
//      case plan: TypedFilter =>
//        val project = plan.asInstanceOf[TypedFilter]
//        resolveLogic(project.child, currentDB, inputTables, outputTables)
//
//      case plan: Distinct =>
//        val project = plan.asInstanceOf[Distinct]
//        resolveLogic(project.child, currentDB, inputTables, outputTables)
//
//      case plan: SubqueryAlias =>
//        val project = plan.asInstanceOf[SubqueryAlias]
//        val childInputTables = new mutable.HashSet[DcTable]()
//        val childOutputTables = new mutable.HashSet[DcTable]()
//
//        resolveLogic(project.child, currentDB, childInputTables, childOutputTables)
//        if (childInputTables.size > 0) {
//          for (table <- childInputTables) inputTables.add(table)
//        } else {
//          inputTables.add(new DcTable(currentDB, project.alias))
//        }
//
////      case plan: UnresolvedCatalogRelation =>
////        val project = plan.asInstanceOf[UnresolvedCatalogRelation]
////        val identifier = project.output.identifier
////        val dcTable = new DcTable(identifier.database.getOrElse(currentDB), identifier.table)
////        inputTables.add(dcTable)
////
////      case plan: UnresolvedRelation =>
////        val project = plan.asInstanceOf[UnresolvedRelation]
////        val dcTable = new DcTable(project.tableIdentifier.database.getOrElse(currentDB), project.tableIdentifier.table)
////        inputTables.add(dcTable)
//
//      case plan: InsertIntoTable =>
//        val project = plan.asInstanceOf[InsertIntoTable]
//        resolveLogic(project.table, currentDB, outputTables, inputTables)
//        resolveLogic(project.child, currentDB, inputTables, outputTables)
//
//      case plan: CreateTable =>
//        val project = plan.asInstanceOf[CreateTable]
//        if (project.query.isDefined) {
//          resolveLogic(project.query.get, currentDB, inputTables, outputTables)
//        }
//        val tableIdentifier = project.tableDesc.identifier
//        val dcTable = new DcTable(tableIdentifier.database.getOrElse(currentDB), tableIdentifier.table)
//        outputTables.add(dcTable)
//
//      case plan: GlobalLimit =>
//        val project = plan.asInstanceOf[GlobalLimit]
//        resolveLogic(project.child, currentDB, inputTables, outputTables)
//
//      case plan: LocalLimit =>
//        val project = plan.asInstanceOf[LocalLimit]
//        resolveLogic(project.child, currentDB, inputTables, outputTables)
//
//      //      case `plan` => logger.info("******child plan******:\n"+plan)
//    }
//  }
}
