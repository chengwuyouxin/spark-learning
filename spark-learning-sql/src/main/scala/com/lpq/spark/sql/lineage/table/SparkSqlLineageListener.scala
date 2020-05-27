package com.lpq.spark.sql.lineage.table

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation
import org.apache.spark.sql.catalyst.catalog.{HiveTableRelation, UnresolvedCatalogRelation}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.execution.datasources.CreateTable
import org.apache.spark.sql.hive.execution.InsertIntoHiveTable
import org.apache.spark.sql.util.QueryExecutionListener

import scala.collection.mutable

/**
  * @author liupengqiang
  * @date 2020/5/12
  */
class SparkSqlLineageListener extends QueryExecutionListener with Logging{
  override def onSuccess(funcName: String, qe: QueryExecution, durationNs: Long): Unit = {
    val result = resolveLogicPlan(qe.analyzed,"default");
    val input:mutable.HashSet[MyTable] = result._1
    val output:mutable.HashSet[MyTable] =result._2
    println("InputTable:")
    input.map(println(_))
    println("OutputTable:")
    output.map(println(_))
  }

  override def onFailure(funcName: String, qe: QueryExecution, exception: Exception): Unit = {
    logError("SparkSqlLineageListener.onFailure")
  }

  def resolveLogicPlan(plan: LogicalPlan, currentDB:String): (mutable.HashSet[MyTable], mutable.HashSet[MyTable]) ={
    val inputTables = new mutable.HashSet[MyTable]()
    val outputTables = new mutable.HashSet[MyTable]()
    resolveLogic(plan, currentDB, inputTables, outputTables)
    Tuple2(inputTables, outputTables)
  }

  def resolveLogic(plan: LogicalPlan, currentDB:String, inputTables:mutable.HashSet[MyTable], outputTables:mutable.HashSet[MyTable]): Unit ={
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
        val childInputTables = new mutable.HashSet[MyTable]()
        val childOutputTables = new mutable.HashSet[MyTable]()

        resolveLogic(project.child, currentDB, childInputTables, childOutputTables)
        if(childInputTables.size > 0){
          for(table <- childInputTables) inputTables.add(table)
        }else{
          inputTables.add(new MyTable(currentDB, project.alias))
        }

      case plan: UnresolvedCatalogRelation =>
        val project = plan.asInstanceOf[UnresolvedCatalogRelation]
        val identifier = project.tableMeta.identifier
        val myTable: MyTable = new MyTable(identifier.database.getOrElse(currentDB), identifier.table)
        inputTables.add(myTable)

      case plan: HiveTableRelation =>
        val project = plan.asInstanceOf[HiveTableRelation]
        val identifier = project.tableMeta.identifier
        val inTable = new MyTable(identifier.database.getOrElse(currentDB),identifier.table)
        inputTables.add(inTable)

      case plan: UnresolvedRelation =>
        val project = plan.asInstanceOf[UnresolvedRelation]
        val myTable: MyTable = new MyTable(project.tableIdentifier.database.getOrElse(currentDB), project.tableIdentifier.table)
        inputTables.add(myTable)

      case plan: InsertIntoTable =>
        val project = plan.asInstanceOf[InsertIntoTable]
        resolveLogic(project.table, currentDB, outputTables, inputTables)
        resolveLogic(project.query, currentDB, inputTables, outputTables)

      case plan: InsertIntoHiveTable =>
        val project = plan.asInstanceOf[InsertIntoHiveTable]
        val tableIdentifier = project.table.identifier
        val myTable = new MyTable(tableIdentifier.database.getOrElse(currentDB), tableIdentifier.table)
        outputTables.add(myTable)
        resolveLogic(project.query,currentDB,inputTables,outputTables)

      case plan: CreateTable =>
        val project = plan.asInstanceOf[CreateTable]
        if(project.query.isDefined){
          resolveLogic(project.query.get, currentDB, inputTables, outputTables)
        }
        val tableIdentifier = project.tableDesc.identifier
        val myTable: MyTable = new MyTable(tableIdentifier.database.getOrElse(currentDB), tableIdentifier.table)
        outputTables.add(myTable)

      case plan: GlobalLimit =>
        val project = plan.asInstanceOf[GlobalLimit]
        resolveLogic(project.child, currentDB, inputTables, outputTables)

      case plan: LocalLimit =>
        val project = plan.asInstanceOf[LocalLimit]
        resolveLogic(project.child, currentDB, inputTables, outputTables)

      case `plan` => logInfo("******child plan******:\n"+plan)
    }
  }
}
