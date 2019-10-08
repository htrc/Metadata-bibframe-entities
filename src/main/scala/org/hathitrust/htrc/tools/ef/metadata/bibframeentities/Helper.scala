package org.hathitrust.htrc.tools.ef.metadata.bibframeentities

import org.hathitrust.htrc.tools.scala.implicits.StringsImplicits._
import org.slf4j.{Logger, LoggerFactory}

import scala.xml.Elem

object Helper {
  @transient lazy val logger: Logger = LoggerFactory.getLogger(Main.appName)

  private val rdfNs = "http://www.w3.org/1999/02/22-rdf-syntax-ns#"

  object LabelTypes {
    val personal = "personal"
    val corporate = "corporate"
  }

  object SubjectTypes {
    val CorporateName = "CorporateName"
    val PersonalName = "PersonalName"
    val ConferenceName = "ConferenceName"
    val FamilyName = "FamilyName"
    val Topic = "Topic"
    val Geographic = "Geographic"
    val Title = "Title"
    val NameTitle = "NameTitle"
    val ComplexSubject = "ComplexSubject"
    val GenreForm = "GenreForm"
    val subjects = "subjects"
    val names = "names"
  }

  import SubjectTypes._
  import EntityTypes._

  private val subjectQueryMap = Map(
    "http://id.loc.gov/ontologies/bibframe/Organization" -> (CorporateName, subjects),
    "http://id.loc.gov/ontologies/bibframe/Person" -> (PersonalName, names),
    "http://id.loc.gov/ontologies/bibframe/Meeting" -> (ConferenceName, names),
    "http://id.loc.gov/ontologies/bibframe/Jurisdiction" -> (CorporateName, names),
    "http://id.loc.gov/ontologies/bibframe/Family" -> (FamilyName, subjects),
    "http://www.loc.gov/mads/rdf/v1#Topic" -> (Topic, subjects),
    "http://www.loc.gov/mads/rdf/v1#Geographic" -> (Geographic, subjects),
    "http://www.loc.gov/mads/rdf/v1#Title" -> (Title, names),
    "http://www.loc.gov/mads/rdf/v1#Name" -> (PersonalName, names),
    "http://www.loc.gov/mads/rdf/v1#NameTitle" -> (NameTitle, names),
    "http://www.loc.gov/mads/rdf/v1#HierarchicalGeographic" -> (ComplexSubject, subjects),
    "http://www.loc.gov/mads/rdf/v1#CorporateName" -> (CorporateName, names),
    "http://www.loc.gov/mads/rdf/v1#ConferenceName" -> (ConferenceName, names),
    "http://www.loc.gov/mads/rdf/v1#ComplexSubject" -> (ComplexSubject, subjects)
  )

  private val componentQueryMap = Map(
    "GenreForm" -> (GenreForm, subjects),
    "Topic" -> (Topic, subjects),
    "Geographic" -> (Geographic, subjects)
  )

  def extractWorldCatEntities(xml: Elem): Set[Entity] =
    (xml \ "Instance").iterator.map(_ \@ s"{$rdfNs}about").map(Entity(WorldCat, _, None, None)).toSet

  def extractViafEntities(xml: Elem): Set[Entity] = {
    import LabelTypes._

    val contributionAgents = xml \ "Work" \ "contribution" \ "Contribution" \ "agent" \ "Agent"
    contributionAgents
      .iterator
      .flatMap { agent =>
        val agentTypes = (agent \ "type").map(_ \@ s"{$rdfNs}resource")
        val label = (agent \ "label").text
        val queryTypes = agentTypes.map { agentType =>
          agentType.takeRightWhile(_ != '/') match {
            case "Person" | "Family" => personal
            case "Organization" | "Meeting" | "Jurisdiction" => corporate
            case _ => agentType
          }
        }

        queryTypes.map(qt => Entity(Viaf, label, None,  Some(qt)))
      }
      .toSet
  }

  def extractLocEntities(xml: Elem): Set[Entity] = {
    val subjects = (xml \ "Work" \ "subject" \ "_").filter(_.label != "Temporal")
    val subjectEntities =
      subjects
        .iterator
        .flatMap { subject =>
          val subjectTypes = (subject \ "type").map(_ \@ s"{$rdfNs}resource")
          val label = (subject \ "label").text
          val queries =  subjectTypes.map(subjectType => subjectQueryMap.getOrElse(subjectType, ("" -> subjectType)))

          queries.map { case (rdfType, queryType) => Entity(Loc, label, Option(rdfType).filter(_.nonEmpty), Some(queryType)) }
        }

    val components = (subjects \ "componentList" \ "_").filter(_.label != "Temporal")
    val componentEntities =
      components
        .iterator
        .map { component =>
          val componentType = component.label
          val label = (component \ "authoritativeLabel").text
          val (rdfType, queryType) = componentQueryMap.getOrElse(componentType, (componentType -> ""))

          Entity(Loc, label, Some(rdfType), Option(queryType).filter(_.nonEmpty))
        }

    (subjectEntities ++ componentEntities).toSet
  }

  def extractEntities(xml: Elem): (Set[Entity], Set[Entity], Set[Entity]) = {
    val worldCatEntities = extractWorldCatEntities(xml)
    val viafEntities = extractViafEntities(xml)
    val locEntities = extractLocEntities(xml)

    (worldCatEntities, viafEntities, locEntities)
  }
}