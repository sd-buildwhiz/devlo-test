package com.buildwhiz.baf2

import com.buildwhiz.infra.{BWMongoDB, BWMongoDB3, DynDoc}
import com.buildwhiz.infra.BWMongoDB3._
import com.buildwhiz.infra.DynDoc._
import com.buildwhiz.utils.BWLogger
import org.apache.http.client.methods.HttpPost
import org.apache.http.entity.StringEntity

import scala.io.Source
import scala.jdk.CollectionConverters._
import org.apache.http.impl.client.HttpClients

import org.bson.Document
import org.bson.types.ObjectId
import org.camunda.bpm.engine.ProcessEngines

import java.util.Calendar

object ProcessApi {

  def listProcesses(db: BWMongoDB=BWMongoDB3): Seq[DynDoc] = {
    db.processes.find()
  }

  def processesByIds(processOids: Seq[ObjectId], db: BWMongoDB = BWMongoDB3): Seq[DynDoc] =
    db.processes.find(Map("_id" -> Map($in -> processOids)))

  def processById(processOid: ObjectId, db: BWMongoDB = BWMongoDB3): DynDoc = {
    db.processes.find(Map("_id" -> processOid)).head
  }

  def exists(processOid: ObjectId, db: BWMongoDB=BWMongoDB3): Boolean = {
    db.processes.find(Map("_id" -> processOid)).nonEmpty
  }

  def allActivityOids(process: DynDoc): Seq[ObjectId] = process.get[Many[ObjectId]]("activity_ids") match {
    case Some(activityOids) => activityOids
    case None => Seq.empty
  }

  def allActivities(processIn: Either[ObjectId, DynDoc], filter: Map[String, Any] = Map.empty,
      db: BWMongoDB=BWMongoDB3): Seq[DynDoc] = {
    val process = processIn match {
      case Right(pr) => pr
      case Left(oid) => processById(oid, db)
    }
    val activityOids = allActivityOids(process)
    ActivityApi.activitiesByIds(activityOids, filter, db)
  }

  def delete(process: DynDoc, db: BWMongoDB = BWMongoDB3): Either[String, String] = {
    val processOid = process._id[ObjectId]
    val isTemplate = process.`type`[String] == "Template"
    val dependantProcessCount: Long = if (isTemplate) {
      db.processes.countDocuments(Map("type" -> "Transient", "template_process_id" -> processOid))
    } else {
      0
    }
    val dependantScheduleCount: Long = if (isTemplate) {
      db.process_schedules.countDocuments(Map("template_process_id" -> processOid))
    } else {
      0
    }
    if (isTemplate && (dependantProcessCount > 0 || dependantScheduleCount > 0)) {
      val dependentCount = dependantProcessCount + dependantScheduleCount
      Left(s"Process '${process.name[String]}' has $dependentCount transient process or schedule dependents")
    } else {
      if (isActive(process)) {
        Left(s"Process '${process.name[String]}' is still active")
      } else {
        db.activity_assignments.deleteMany(Map("process_id" -> processOid))
        val processDeleteResult = db.processes.deleteOne(Map("_id" -> processOid))
        if (processDeleteResult.getDeletedCount == 0) {
          Left(s"MongoDB error: $processDeleteResult")
        } else {
          val phaseUpdateResult = db.phases.updateOne(Map("process_ids" -> processOid),
            Map("$pull" -> Map("process_ids" -> processOid)))
          val activityOids: Seq[ObjectId] = allActivities(Right(process), db = db).map(_._id[ObjectId])
          val activityDeleteCount = if (activityOids.nonEmpty) {
            db.tasks.deleteMany(Map("_id" -> Map("$in" -> activityOids))).getDeletedCount
          } else {
            0
          }
          val deliverables: Seq[DynDoc] = db.deliverables.find(Map("activity_id" -> Map("$in" -> activityOids)))
          val deliverableOids = deliverables.map(_._id[ObjectId])
          val deliverableDeleteCount = if (deliverables.nonEmpty) {
            db.deliverables.deleteMany(Map("_id" -> Map("$in" -> deliverableOids))).getDeletedCount
          } else {
            0
          }
          val constraintDeleteCount = if (deliverables.nonEmpty) {
            db.constraints.deleteMany(Map("owner_deliverable_id" -> Map("$in" -> deliverableOids))).getDeletedCount
          } else {
            0
          }
          val message = s"Deleted process '${process.name[String]}' (${process._id[ObjectId]}). " +
            s"Also updated ${phaseUpdateResult.getModifiedCount} phase records, " +
            s"and deleted $activityDeleteCount activities, $deliverableDeleteCount deliverables, $constraintDeleteCount constraints"
          Right(message)
        }
      }
    }

  }

  def parentPhase(processOid: ObjectId, db: BWMongoDB=BWMongoDB3): DynDoc = {
    db.phases.find(Map("process_ids" -> processOid)).head
  }

  def canDelete(process: DynDoc): Boolean = !isActive(process)

  def canLaunch(process: DynDoc, parentPhase: DynDoc, user: DynDoc): Boolean = {
    val allActivitiesAssigned = ProcessApi.allActivityOids(process).forall(activityOid => {
      val assignments = ActivityApi.teamAssignment.list(activityOid)
      assignments.forall(_.has("person_id"))
    })
    allActivitiesAssigned && PhaseApi.canManage(user._id[ObjectId], parentPhase) &&
        process.status[String] == "defined"
  }

  def isActive(process: DynDoc): Boolean = {
    //val processName = process.name[String]
    if (process.has("process_instance_id")) {
      val rts = ProcessEngines.getDefaultProcessEngine.getRuntimeService
      val query = rts.createProcessInstanceQuery()
      val instance = query.processInstanceId(process.process_instance_id[String]).active().singleResult()
      val found = instance != null
      //val message = s"Process '$processName' has 'process_instance_id' field, Active instances found: $found"
      //BWLogger.log(getClass.getName, "isActive", message)
      found
    } else {
      //val message = s"Process '$processName' has no 'process_instance_id'"
      //BWLogger.log(getClass.getName, "isActive", message)
      false
    }
  }

  def isHealthy(process: DynDoc): Boolean = (process.status[String] == "running") == isActive(process)

  def hasRole(personOid: ObjectId, process: DynDoc): Boolean = {
    isAdmin(personOid, process) || allActivities(Right(process)).exists(activity => ActivityApi.hasRole(personOid, activity))
  }

  def isAdmin(personOid: ObjectId, process: DynDoc): Boolean =
    process.admin_person_id[ObjectId] == personOid

  def isManager(personOid: ObjectId, process: DynDoc): Boolean = process.assigned_roles[Many[Document]].
    exists(ar => ar.person_id[ObjectId] == personOid &&
      ar.role_name[String].matches("(?i)(?:project-|phase-|process-)?manager"))

  def canManage(personOid: ObjectId, process: DynDoc): Boolean =
      isManager(personOid, process) || isAdmin(personOid, process) ||
      PhaseApi.canManage(personOid, parentPhase(process._id[ObjectId]))

  def isZombie(process: DynDoc): Boolean = process.has("is_zombie") && process.is_zombie[Boolean]

  def displayStatus(process: DynDoc): String = {
    if (isActive(process)) {
      "active"
    } else {
      val timestamps: DynDoc = process.timestamps[Document]
      if (timestamps.has("end"))
        "ended"
      else if (timestamps.has("start"))
        "error"
      else
        "dormant"
    }
  }

  def managers(process: DynDoc): Seq[ObjectId] = {
    val phase = parentPhase(process._id[ObjectId])
    val phaseManagers = PhaseApi.managers(Right(phase))
    val processManagers = process.assigned_roles[Many[Document]].
      filter(_.role_name[String].matches(".*(?i)manager")).map(_.person_id[ObjectId])
    (phaseManagers ++ processManagers).distinct
  }

  def timeZone(process: DynDoc): String = {
    if (process.has("tz")) {
      process.tz[String]
    } else {
      val managerOids = managers(process)
      PersonApi.personsByIds(managerOids).map(_.tz[String]).groupBy(t => t).
        map(p => (p._1, p._2.length)).reduce((a, b) => if (a._2 > b._2) a else b)._1
    }
  }

  def validateNewName(newName: String, parentPhaseOid: ObjectId, db: BWMongoDB=BWMongoDB3): Boolean = {
    val nameLength = newName.length
    if (newName.trim.length != nameLength)
      throw new IllegalArgumentException(s"Bad process name (has blank padding): '$newName'")
    if (nameLength > 150 || nameLength < 3)
      throw new IllegalArgumentException(s"Bad process name length: $nameLength (must be 3-150)")
    val siblingProcessOids: Seq[ObjectId] = PhaseApi.allProcessOids(PhaseApi.phaseById(parentPhaseOid))
    val count = db.processes.countDocuments(Map("name" -> newName, "_id" -> Map($in -> siblingProcessOids)))
    if (count > 0)
      throw new IllegalArgumentException(s"Process named '$newName' already exists")
    true
  }

  def checkProcessSchedules(scheduleMs: Long, db: BWMongoDB=BWMongoDB3): Unit = {
    //BWLogger.log(getClass.getName, "LOCAL", s"ENTRY-checkProcessSchedules")
    def createNewTransientProcess(schedule: DynDoc): Unit = {
      def createRequest(schedule: DynDoc): Option[HttpPost] = {
        val systemUser = PersonApi.systemUser()
        val parameterEntity = Document.parse(schedule.asDoc.toJson)
        if (!parameterEntity.containsKey("template_parameters")) {
          parameterEntity.append("template_parameters", Seq.empty[Any].asJava)
        }
        def oid2string(oid: Any): Any = oid.toString
        def s2s(s: Any): Any = s
        def ms2string(ms: Any): Any = {
          val calendar = Calendar.getInstance()
          calendar.setTimeInMillis(ms.asInstanceOf[Long])
          "%02d/%02d/%4d".format(calendar.get(Calendar.MONTH) + 1, calendar.get(Calendar.DAY_OF_MONTH),
              calendar.get(Calendar.YEAR))
        }
        def tp2s(tp: Any): Seq[Any] = {
          tp.asInstanceOf[Many[Document]].map(p => {
            val newDoc = new Document("name", p.name[String]).append("type", p.`type`[String]).
              append("runtime", p.runtime[Boolean]).append("value",
              p.`type`[String] match {
                case "Text" => p.value[Any]
                case "Boolean" => p.value[Any] match {
                  case b: Boolean => Map(true -> "1", false -> "0")(b)
                  case x => x
                }
                case "Unit" => p.value[Any] match {
                  case oid: ObjectId => oid.toString
                  case x => x
                }
                case "Date and Time" => p.value[Any] match {
                  case ms: Long =>
                    val cal = Calendar.getInstance()
                    cal.setTimeInMillis(ms)
                    "%02d/%02d/%4d %02d:%02d:%02d".format(cal.get(Calendar.DAY_OF_MONTH),
                      cal.get(Calendar.MONTH) + 1, cal.get(Calendar.YEAR), cal.get(Calendar.HOUR_OF_DAY),
                      cal.get(Calendar.MINUTE), cal.get(Calendar.SECOND))
                  case x => x.toString
                }
                case "List" => p.value[Any]
                case "Number" => p.value[Any] match {
                  case nbr: BigDecimal => nbr.toDouble.toString
                  case nbr: BigInt => nbr.toDouble.toString
                  case nbr: Number => nbr.doubleValue().toString
                  case nbr: Float => nbr.toString
                  case nbr: Int => nbr.toString
                  case nbr: Double => nbr.toString
                  case nbr: Long => nbr.toString
                  case x => x.toString
                }
                case "Date" => p.value[Any] match {
                  case msl: Long => ms2string(msl)
                  case msl: Int => ms2string(msl)
                  case x => x.toString
                }
              })
            if (p.`type`[String] == "List") {
              newDoc.append("enum_definition", p.enum_definition[String])
            }
            newDoc
          })
        }
        val requiredKeyInfos: Map[String, Any => Any] = Seq(
          ("project_id", oid2string _), ("phase_id", oid2string _), ("template_process_id", oid2string _),
          ("title", s2s _), ("priority", s2s _), ("template_parameters", tp2s _)).toMap
        val missingParameters = requiredKeyInfos.keys.filterNot(parameterEntity.containsKey)
        if (missingParameters.nonEmpty) {
          throw new IllegalArgumentException(s"Missing parameters: ${missingParameters.toSeq.mkString(", ")}")
        }
        def validate(key: String, value: Any): Boolean = {
          val isValid = (key, value) match {
            case ("project_id", projectOid: String) => ProjectApi.exists(new ObjectId(projectOid))
            case ("phase_id", phaseOid: String) => PhaseApi.exists(new ObjectId(phaseOid))
            case ("template_process_id", templateProcessOid: String) =>
              ProcessApi.exists(new ObjectId(templateProcessOid))
            // case ("template_parameters", templateParameters: Seq[Any]) => templateParameters.nonEmpty
            case _ => true
          }
          isValid
        }
        val parmEntityValid: Document = parameterEntity.entrySet().asScala.
            filter(e => requiredKeyInfos.contains(e.getKey)).map(e => {
          val (key, value) = (e.getKey, e.getValue)
          val newValue = requiredKeyInfos(key)(value)
          (key, (validate(key, newValue), newValue))
        }).toMap

        val badParamNames: Seq[String] = parmEntityValid.asScala.toSeq.map(_.asInstanceOf[(String, (Boolean, Any))]).
          filterNot(_._2._1).map(_._1)

        val optRequest = if (badParamNames.nonEmpty) {
          val message: String = s"""Bad value for parameter(s): ${badParamNames.mkString(", ")}"""
          BWLogger.log(getClass.getName, "LOCAL", s"ERROR: checkProcessSchedules " +
            s"${schedule.title[String]} (${schedule._id[ObjectId]}) -> $message")
          db.process_schedules.updateOne(Map("_id" -> schedule._id[ObjectId]),
            Map($set -> Map("timestamps.last_failure" -> scheduleMs,
              "timestamps.last_message" -> message)))
          None
        } else {
          val newParamEntities: Document = parmEntityValid.asScala.toSeq.map(_.asInstanceOf[(String, (Boolean, Any))]).
            map(e => (e._1, e._2._2)).toMap
          val parameterEntityJson = newParamEntities.toJson
          val request = new HttpPost(s"http://localhost:3000/ProcessClone7?uid=${systemUser._id[ObjectId]}")
          request.setHeader("Content-Type", "application/json; charset=utf-8")
          request.setEntity(new StringEntity(parameterEntityJson))
          Some(request)
        }
        optRequest
      }
      val t0 = System.currentTimeMillis()
      createRequest(schedule) match {
        case Some(postRequest) =>
          val nodeResponse = HttpClients.createDefault().execute(postRequest)
          val delay = System.currentTimeMillis() - t0
          val nodeEntity = nodeResponse.getEntity
          val nodeEntityString = Source.fromInputStream(nodeEntity.getContent).getLines().mkString("\n")
          val nodeEntityDoc: DynDoc = Document.parse(nodeEntityString)
          if (nodeEntityDoc.ok[Int] == 1) {
            val calendar = Calendar.getInstance()
            calendar.setTimeInMillis(scheduleMs)
            val runNext: Long = schedule.frequency[String].toLowerCase match {
              case "quarter-hourly" => calendar.add(Calendar.MINUTE, 15); calendar.getTimeInMillis
              case "half-hourly" => calendar.add(Calendar.MINUTE, 30); calendar.getTimeInMillis
              case "hourly" => calendar.add(Calendar.HOUR, 1); calendar.getTimeInMillis
              case "twice-daily" => calendar.add(Calendar.HOUR, 12); calendar.getTimeInMillis
              case "daily" => calendar.add(Calendar.DAY_OF_MONTH, 1); calendar.getTimeInMillis
              case "weekly" => calendar.add(Calendar.DAY_OF_MONTH, 7); calendar.getTimeInMillis
              case "monthly" => calendar.add(Calendar.MONTH, 1); calendar.getTimeInMillis
              case "quarterly" => calendar.add(Calendar.MONTH, 3); calendar.getTimeInMillis
              case "half-yearly" => calendar.add(Calendar.MONTH, 6); calendar.getTimeInMillis
              case "yearly" => calendar.add(Calendar.YEAR, 1); calendar.getTimeInMillis
            }
            db.process_schedules.updateOne(Map("_id" -> schedule._id[ObjectId]),
              Map($set -> Map("timestamps.run_next" -> runNext, "timestamps.last_success" -> scheduleMs,
                "timestamps.last_message" -> nodeEntityDoc.message[String])))
            BWLogger.log(getClass.getName, "LOCAL", s"AUDIT-checkProcessSchedules(time: $delay): " +
              s"${schedule.title[String]} (${schedule._id[ObjectId]}) -> $runNext")
          } else {
            db.process_schedules.updateOne(Map("_id" -> schedule._id[ObjectId]),
              Map($set -> Map("timestamps.last_failure" -> scheduleMs,
                "timestamps.last_message" -> nodeEntityDoc.message[String])))
            BWLogger.log(getClass.getName, "LOCAL", s"ERROR-checkProcessSchedules(time: $delay): " +
              s"${schedule.title[String]} (${schedule._id[ObjectId]}) -> $nodeEntityString")
          }
        case None =>
      }
    }

    val readySchedules: Seq[DynDoc] = db.process_schedules.
      find(Map("timestamps.run_next" -> Map($lte -> scheduleMs), "timestamps.end" -> Map($gte -> scheduleMs)))
    //BWLogger.log(getClass.getName, "LOCAL", s"AUDIT-???-checkProcessSchedules ready-schedules: ${readySchedules.length}")
    for (schedule <- readySchedules) {
      try {
        val timestamps: DynDoc = schedule.timestamps[Document]
        if (timestamps.has("last_failure")) {
          BWLogger.log(getClass.getName, "LOCAL", s"ERROR-checkProcessSchedules (SKIPPING due to prior error): " +
            s"'${schedule.title[String]}' (${schedule._id[ObjectId]})")
        } else {
          createNewTransientProcess(schedule)
        }
      } catch {
        case t: Throwable =>
          BWLogger.log(getClass.getName, "LOCAL", s"ERROR-checkProcessSchedules: " +
            s"${schedule.title[String]} (${schedule._id[ObjectId]}) -> ${t.getClass.getSimpleName}(${t.getMessage})")
      }
    }
  }

  def main(args: Array[String]): Unit = {
    val processOid = new ObjectId("6464737c1540061c1491c76a")
    val found = ProcessApi.exists(processOid)
    println(processOid, found)
  }

}