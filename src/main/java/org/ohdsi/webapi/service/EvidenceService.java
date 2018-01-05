package org.ohdsi.webapi.service;

import java.util.Collection;
import java.util.Date;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import java.io.IOException;
import java.math.BigDecimal;
import javax.ws.rs.Consumes;

import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import org.apache.commons.httpclient.util.URIUtil;
import org.apache.commons.lang3.StringUtils;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.ohdsi.circe.helper.ResourceHelper;
import org.ohdsi.sql.SqlRender;
import org.ohdsi.sql.SqlTranslate;
import org.ohdsi.webapi.conceptset.ConceptSetGenerationInfoRepository;
//import org.ohdsi.webapi.cohortanalysis.CohortAnalysisTasklet;
import org.ohdsi.webapi.evidence.CohortStudyMapping;
import org.ohdsi.webapi.evidence.CohortStudyMappingRepository;
import org.ohdsi.webapi.evidence.ConceptCohortMapping;
import org.ohdsi.webapi.evidence.ConceptCohortMappingRepository;
import org.ohdsi.webapi.evidence.ConceptOfInterestMapping;
import org.ohdsi.webapi.evidence.ConceptOfInterestMappingRepository;
import org.ohdsi.webapi.evidence.DrugEvidence;
import org.ohdsi.webapi.evidence.EvidenceDetails;
import org.ohdsi.webapi.evidence.EvidenceSummary;
import org.ohdsi.webapi.evidence.EvidenceUniverse;
import org.ohdsi.webapi.evidence.HoiEvidence;
import org.ohdsi.webapi.evidence.DrugHoiEvidence;
import org.ohdsi.webapi.evidence.DrugLabel;
import org.ohdsi.webapi.evidence.DrugLabelRepository;
import org.ohdsi.webapi.evidence.EvidenceInfo;
import org.ohdsi.webapi.evidence.DrugRollUpEvidence;
import org.ohdsi.webapi.evidence.Evidence;
import org.ohdsi.webapi.evidence.LinkoutData;
import org.ohdsi.webapi.evidence.SpontaneousReport;
import org.ohdsi.webapi.evidence.EvidenceSearch;
import org.ohdsi.webapi.evidence.NegativeControl;
import org.ohdsi.webapi.evidence.NegativeControlRecord;
import org.ohdsi.webapi.evidence.NegativeControlRepository;
import org.ohdsi.webapi.evidence.NegativeControlTasklet;
import org.ohdsi.webapi.job.JobExecutionResource;
import org.ohdsi.webapi.job.JobTemplate;
import org.ohdsi.webapi.source.Source;
import org.ohdsi.webapi.source.SourceDaimon;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

/**
 * @author rkboyce based on the vocabulary service written by fdefalco and an
 * initial api written by m_rasteger
 */
@Path("/evidence")
@Component
public class EvidenceService extends AbstractDaoService {
  @Autowired
  private JobTemplate jobTemplate;
        
  @Autowired
  private DrugLabelRepository drugLabelRepository;

  @Autowired
  private ConceptCohortMappingRepository mappingRepository;
  
  @Autowired
  private ConceptOfInterestMappingRepository conceptOfInterestMappingRepository;

  @Autowired
  private CohortStudyMappingRepository cohortStudyMappingRepository;
  
  @Autowired
  private NegativeControlRepository negativeControlRepository;
  
  @Autowired
  private ConceptSetGenerationInfoRepository conceptSetGenerationInfoRepository;
  
  @GET
  @Path("study/{cohortId}")
  @Produces(MediaType.APPLICATION_JSON)
  public Collection<CohortStudyMapping> getCohortStudyMapping(@PathParam("cohortId") int cohortId) {
    return cohortStudyMappingRepository.findByCohortDefinitionId(cohortId);
  }

  @GET
  @Path("mapping/{conceptId}")
  @Produces(MediaType.APPLICATION_JSON)
  public Collection<ConceptCohortMapping> getConceptCohortMapping(@PathParam("conceptId") int conceptId) {
    return mappingRepository.findByConceptId(conceptId);
  }

  @GET
  @Path("conceptofinterest/{conceptId}")
  @Produces(MediaType.APPLICATION_JSON)
  public Collection<ConceptOfInterestMapping> getConceptOfInterest(@PathParam("conceptId") int conceptId) {
    return conceptOfInterestMappingRepository.findAllByConceptId(conceptId);
  }
  
  @GET
  @Path("label/{setid}")
  @Produces(MediaType.APPLICATION_JSON) 
  public Collection<DrugLabel> getDrugLabels(@PathParam("setid") String setid) {
    return drugLabelRepository.findAllBySetid(setid);
  }
  
  @GET
  @Path("labelsearch/{searchTerm}")
  @Produces(MediaType.APPLICATION_JSON) 
  public Collection<DrugLabel> searchDrugLabels(@PathParam("searchTerm") String searchTerm) {
    return drugLabelRepository.searchNameContainsTerm(searchTerm);
  }
  
  
  @GET
  @Path("{sourceKey}/info")
  @Produces(MediaType.APPLICATION_JSON)
  public Collection<EvidenceInfo> getInfo(@PathParam("sourceKey") String sourceKey) {

    Source source = getSourceRepository().findBySourceKey(sourceKey);
    String tableQualifier = source.getTableQualifier(SourceDaimon.DaimonType.Evidence);
    
    String sql_statement = ResourceHelper.GetResourceAsString("/resources/evidence/sql/getInfo.sql");
    sql_statement = SqlRender.renderSql(sql_statement, new String[]{"OHDSI_schema"}, new String[]{tableQualifier});
    sql_statement = SqlTranslate.translateSql(sql_statement, "sql server", source.getSourceDialect());  // TODO: why is 'sql server' string passed here?

    final List<EvidenceInfo> infoOnSources = new ArrayList<EvidenceInfo>();

    //try {
    List<Map<String, Object>> rows = getSourceJdbcTemplate(source).queryForList(sql_statement);
        //} catch (EmptyResultDataAccessException e) {
    //    log.debug(String.format("Request for conceptId=%s resulted in 0 results", id));
    //    throw new WebApplicationException(Response.Status.RESET_CONTENT); // http 205
    //}

    for (Map rs : rows) {
      EvidenceInfo info = new EvidenceInfo();
      info.title = (String) rs.get("TITLE");
      info.description = (String) rs.get("DESCRIPTION");
      info.contributer = (String) rs.get("CONTRIBUTER");
      info.creator = (String) rs.get("CREATOR");
      info.creationDate = (Date) rs.get("CREATION_DATE");
      info.rights = (String) rs.get("RIGHTS");
      info.source = (String) rs.get("SOURCE");
      infoOnSources.add(info);
    }
    return infoOnSources;
  }

  /**
   * @param id
   * @return
   */
  @GET
  @Path("{sourceKey}/drug/{id}")
  @Produces(MediaType.APPLICATION_JSON)
  public Collection<DrugEvidence> getDrugEvidence(@PathParam("sourceKey") String sourceKey, @PathParam("id") final Long id) {
    Source source = getSourceRepository().findBySourceKey(sourceKey);
    String tableQualifier = source.getTableQualifier(SourceDaimon.DaimonType.Evidence);
    
    String sql_statement = ResourceHelper.GetResourceAsString("/resources/evidence/sql/getDrugEvidence.sql");

    sql_statement = SqlRender.renderSql(sql_statement, new String[]{"id", "OHDSI_schema"},
            new String[]{String.valueOf(id), tableQualifier});
    sql_statement = SqlTranslate.translateSql(sql_statement, "sql server", source.getSourceDialect());

    final List<DrugEvidence> drugEvidences = new ArrayList<DrugEvidence>();

    //try {
    List<Map<String, Object>> rows = getSourceJdbcTemplate(source).queryForList(sql_statement);
        //} catch (EmptyResultDataAccessException e) {
    //    log.debug(String.format("Request for conceptId=%s resulted in 0 results", id));
    //    throw new WebApplicationException(Response.Status.RESET_CONTENT); // http 205
    //}
    String tempDrugHoi = "";
    String tempEvidenceType = "";
    Character tempSupports = new Character('f');
    for (Map rs : rows) {

      String evi_type = (String) rs.get("EV_TYPE");

      Character supports = null;
      if (rs.get("EV_SUPPORTS") == null){
	  supports = new Character('u');
      } else if ((String) rs.get("EV_SUPPORTS") == "t"){
	  supports = new Character('t');
      } else {
	  supports = new Character('f');
      }
      
      String linkout = (String) rs.get("EV_LINKOUT");
      String hoi = String.valueOf((Integer) rs.get("EV_HOI"));
      String hoiName = (String)rs.get("EV_SNOMED_HOI");
      String statType = (String) rs.get("EV_STAT_TYPE");
      BigDecimal statVal = (BigDecimal) rs.get("EV_STAT_VAL");
      String drugHoi = (String)rs.get("EV_DRUGHOI");
      
      if((!drugHoi.equalsIgnoreCase(tempDrugHoi))||(!evi_type.equalsIgnoreCase(tempEvidenceType))||(!(Character.toLowerCase(supports) == (Character.toLowerCase(tempSupports)))))
      {
      DrugEvidence evidence = new DrugEvidence();
      tempDrugHoi = drugHoi;
      tempEvidenceType = evi_type;
      tempSupports = supports;
      //evidence.drughoi = drugHoi;
      evidence.evidence = evi_type;
      evidence.supports = supports;
      evidence.linkout = linkout;
      evidence.hoi = hoi;
      evidence.hoiName = hoiName;
      evidence.statisticType = statType;
      if (statType.equals("COUNT")) {
        evidence.count = statVal.intValue();
      } else {
        evidence.value = statVal;
      }
      
      drugEvidences.add(evidence);
      }
    }
    return drugEvidences;
  }

  /**
   * @param id
   * @return
   */
  @GET
  @Path("{sourceKey}/hoi/{id}")
  @Produces(MediaType.APPLICATION_JSON)
  public Collection<HoiEvidence> getHoiEvidence(@PathParam("sourceKey") String sourceKey, @PathParam("id") final Long id) {
    
    Source source = getSourceRepository().findBySourceKey(sourceKey);
    String tableQualifier = source.getTableQualifier(SourceDaimon.DaimonType.Evidence);
    
    String sql_statement = ResourceHelper.GetResourceAsString("/resources/evidence/sql/getHoiEvidence.sql");
    sql_statement = SqlRender.renderSql(sql_statement, new String[]{"id", "OHDSI_schema"},
            new String[]{String.valueOf(id), tableQualifier});
    sql_statement = SqlTranslate.translateSql(sql_statement, "sql server", source.getSourceDialect()); 

    final List<HoiEvidence> hoiEvidences = new ArrayList<HoiEvidence>();

    //try {
    List<Map<String, Object>> rows = getSourceJdbcTemplate(source).queryForList(sql_statement);
        //} catch (EmptyResultDataAccessException e) {
    //    log.debug(String.format("Request for conceptId=%s resulted in 0 results", id));
    //    throw new WebApplicationException(Response.Status.RESET_CONTENT); // http 205
    //}

    for (Map rs : rows) {
      String evi_type = (String) rs.get("EV_TYPE");

      Character supports = null;
      if (rs.get("EV_SUPPORTS") == null){
	  supports = new Character('u');
      } else if ((String) rs.get("EV_SUPPORTS") == "t"){
	  supports = new Character('t');
      } else {
	  supports = new Character('f');
      }

      String linkout = (String) rs.get("EV_LINKOUT");
      String drug = String.valueOf((Integer) rs.get("EV_DRUG"));
      String drugName = (String) rs.get("EV_RXNORM_DRUG");
      String statType = (String) rs.get("EV_STAT_TYPE");
      BigDecimal statVal = (BigDecimal) rs.get("EV_STAT_VAL");

      HoiEvidence evidence = new HoiEvidence();
      evidence.evidence = evi_type;
      evidence.supports = supports;
      evidence.linkout = linkout;
      evidence.drug = drug;
      evidence.drugName = drugName;
      evidence.statisticType = statType;
      if (statType.equals("COUNT")) {
        evidence.count = statVal.intValue();
      } else {
        evidence.value = statVal;
      }

      hoiEvidences.add(evidence);
    }
    return hoiEvidences;
  }

  /**
   * @param id
   * @return
   */
  @GET
  @Path("{sourceKey}/drughoi/{key}")
  @Produces(MediaType.APPLICATION_JSON)
  public Collection<DrugHoiEvidence> getDrugHoiEvidence(@PathParam("sourceKey") String sourceKey, @PathParam("key") final String key) {
    String[] par = key.split("-");
    String drug_id = par[0];
    String hoi_id = par[1];
    
    Source source = getSourceRepository().findBySourceKey(sourceKey);
    String tableQualifier = source.getTableQualifier(SourceDaimon.DaimonType.Evidence);
    
    String sql_statement = ResourceHelper.GetResourceAsString("/resources/evidence/sql/getDrugHoiEvidence.sql");
    sql_statement = SqlRender.renderSql(sql_statement, new String[]{"drug_id", "hoi_id", "OHDSI_schema"},
            new String[]{drug_id, hoi_id, tableQualifier});
    sql_statement = SqlTranslate.translateSql(sql_statement, "sql server", source.getSourceDialect()); // TODO: why is 'sql server' string passed here?

    final List<DrugHoiEvidence> evidences = new ArrayList<DrugHoiEvidence>();

    //try {
    List<Map<String, Object>> rows = getSourceJdbcTemplate(source).queryForList(sql_statement);
        //} catch (EmptyResultDataAccessException e) {
    //    log.debug(String.format("Request for conceptId=%s resulted in 0 results", id));
    //    throw new WebApplicationException(Response.Status.RESET_CONTENT); // http 205
    //}

    for (Map rs : rows) {
      String evi_type = (String) rs.get("EV_TYPE");
      Character supports = null;
      if (rs.get("EV_SUPPORTS") == null){
	  supports = new Character('u');
      } else if ((String) rs.get("EV_SUPPORTS") == "t"){
	  supports = new Character('t');
      } else {
	  supports = new Character('f');
      }

      String linkout = (String) rs.get("EV_LINKOUT");
      String statType = (String) rs.get("EV_STAT_TYPE");
      BigDecimal statVal = (BigDecimal) rs.get("EV_STAT_VAL");

      DrugHoiEvidence evidence = new DrugHoiEvidence();
      evidence.evidence = evi_type;
      evidence.supports = supports;
      evidence.linkout = linkout;
      evidence.statisticType = statType;
      if (statType.equals("COUNT")) {
        evidence.count = statVal.intValue();
      } else {
        evidence.value = statVal;
      }

      evidences.add(evidence);
    }
    return evidences;
  }

  /**
   * @param id
   * @param filter
   * @return
   */
  @GET
  @Path("{sourceKey}/drugrollup/{filter}/{id}")
  @Produces(MediaType.APPLICATION_JSON)
  public Collection<DrugRollUpEvidence> getDrugRollupIngredientEvidence(@PathParam("sourceKey") String sourceKey, @PathParam("id") final Long id, @PathParam("filter") final String filter) {
    
    Source source = getSourceRepository().findBySourceKey(sourceKey);
    String evidenceTableQualifier = source.getTableQualifier(SourceDaimon.DaimonType.Evidence);
    String vocabularyTableQualifier = source.getTableQualifier(SourceDaimon.DaimonType.Vocabulary);
    
    String sql_statement = "";
    if (filter.equals("ingredient")) {
      sql_statement = ResourceHelper.GetResourceAsString("/resources/evidence/sql/getDrugEvidenceRollupByIngredient.sql");
    } else if (filter.equals("clinicaldrug")) {
      sql_statement = ResourceHelper.GetResourceAsString("/resources/evidence/sql/getDrugEvidenceRollupByClinicalDrug.sql");
    } else if (filter.equals("brandeddrug")) {
      sql_statement = ResourceHelper.GetResourceAsString("/resources/evidence/sql/getDrugEvidenceRollupByBrandedDrug.sql");
    } else {
      log.debug(String.format("The call did not specify an appropriate roll up filter (e.g., ingredient, clinicaldrug, brandeddrug)", id));
      return null;
    }

    sql_statement = SqlRender.renderSql(sql_statement, new String[]{"id", "OHDSI_schema", "CDM_schema"},
            new String[]{String.valueOf(id), evidenceTableQualifier, vocabularyTableQualifier});
    sql_statement = SqlTranslate.translateSql(sql_statement, "sql server", source.getSourceDialect()); // TODO: why is 'sql server' string passed here?

    final List<DrugRollUpEvidence> drugEvidences = new ArrayList<DrugRollUpEvidence>();

    //try {
    List<Map<String, Object>> rows = getSourceJdbcTemplate(source).queryForList(sql_statement);
        //} catch (EmptyResultDataAccessException e) {
    //    log.debug(String.format("Request for conceptId=%s resulted in 0 results", id));
    //    throw new WebApplicationException(Response.Status.RESET_CONTENT); // http 205
    //}

    for (Map rs : rows) {
      DrugRollUpEvidence evidence = new DrugRollUpEvidence();
      evidence.reportName = (String) rs.get("REPORT_NAME");
      evidence.ingredientId = (Integer) rs.get("INGREDIENT_ID");
      evidence.ingredientName = (String) rs.get("INGREDIENT");
      evidence.clinicalDrugId = (Integer) rs.get("CLINICAL_DRUG_ID");
      evidence.clinicalDrugName = (String) rs.get("CLINICAL_DRUG");
      evidence.hoiId = (Integer) rs.get("HOI_ID");
      evidence.hoiName = (String) rs.get("HOI");
      evidence.pubmedMeshCTcount = (Integer) rs.get("MEDLINE_CT_COUNT");
      evidence.pubmedMeshCaseReportcount = (Integer) rs.get("MEDLINE_CASE_COUNT");
      evidence.pubmedMeshOthercount = (Integer) rs.get("MEDLINE_OTHER_COUNT");
      evidence.ctdChemicalDiseaseCount = (Integer) rs.get("CTD_CHEMICAL_DISEASE_COUNT");
      evidence.splicerCount = (Integer) rs.get("SPLICER_COUNT");
      evidence.euSPCcount = (Integer) rs.get("EU_SPC_COUNT");
      evidence.semmedCTcount = (Integer) rs.get("SEMMEDDB_CT_COUNT");
      evidence.semmedCaseReportcount = (Integer) rs.get("SEMMEDDB_CASE_COUNT");
      evidence.semmedOthercount = (Integer) rs.get("SEMMEDDB_OTHER_COUNT");
      evidence.semmedNegCTcount = (Integer) rs.get("SEMMEDDB_NEG_CT_COUNT");
      evidence.semmedNegCaseReportcount = (Integer) rs.get("SEMMEDDB_NEG_CASE_COUNT");
      evidence.semmedNegOthercount = (Integer) rs.get("SEMMEDDB_NEG_OTHER_COUNT");
      evidence.aersReportCount = (Integer) rs.get("AERS_REPORT_COUNT");
      evidence.prr = (BigDecimal) rs.get("PRR");
      
      drugEvidences.add(evidence);
    }
    return drugEvidences;
  }
  
  
  @GET
  @Path("{sourceKey}/{id}")
  @Produces(MediaType.APPLICATION_JSON)
  public Collection<Evidence> getEvidence(@PathParam("sourceKey") String sourceKey, @PathParam("id") final Long id) {
    
    Source source = getSourceRepository().findBySourceKey(sourceKey);
    String tableQualifier = source.getTableQualifier(SourceDaimon.DaimonType.Evidence);
    
    String sql_statement = ResourceHelper.GetResourceAsString("/resources/evidence/sql/getEvidence.sql");
    sql_statement = SqlRender.renderSql(sql_statement, new String[]{"id", "tableQualifier"},
            new String[]{String.valueOf(id), tableQualifier});
    sql_statement = SqlTranslate.translateSql(sql_statement, "sql server", source.getSourceDialect()); 

    final List<Evidence> evidences = new ArrayList<Evidence>();

    List<Map<String, Object>> rows = getSourceJdbcTemplate(source).queryForList(sql_statement);
    
    for (Map rs : rows) {
      Evidence e = new Evidence();
      e.conditionId = (Integer) rs.get("CONDITION_ID");
      e.drugId = (Integer) rs.get("DRUG_ID");
      e.drugName = (String) rs.get("DRUG_NAME");
      e.conditionName = (String) rs.get("CONDITION_NAME");
      e.evidenceType = (String) rs.get("EVIDENCE_TYPE");

      if (rs.get("SUPPORTS") == null){
	  e.supports = new Character('u');
      } else if (((String) rs.get("SUPPORTS")).equalsIgnoreCase("t")){
	  e.supports = new Character('t');
      } else {
	  e.supports = new Character('f');
      }

      e.statisticType = (String) rs.get("STATISTIC_TYPE");
      e.linkout = (String) rs.get("EVIDENCE_LINKOUT");
      e.value = (BigDecimal) rs.get("STATISTIC_VALUE");

      evidences.add(e);
    }
    return evidences;
  }  
  
  

  
  /**
   * @param conditionID
   * @param drugID
   * @param evidenceGroup
   * @return
   */
  @GET
  @Path("{sourceKey}/evidencesummary")
  @Produces(MediaType.APPLICATION_JSON)
  public Collection<EvidenceSummary> getEvidenceSummaryBySource(@PathParam("sourceKey") String sourceKey, @QueryParam("conditionID") String conditionID, @QueryParam("drugID") String drugID, @QueryParam("evidenceGroup") String evidenceGroup) {
	  Source source = getSourceRepository().findBySourceKey(sourceKey);
	  String tableQualifier = source.getTableQualifier(SourceDaimon.DaimonType.Evidence);
	  String evidenceType = null;
	  if(evidenceGroup.equalsIgnoreCase("Literature"))
	  		evidenceType = "MEDLINE";
	  String sql_statement = ResourceHelper.GetResourceAsString("/resources/evidence/sql/getEvidenceSummaryBySource.sql");
	  sql_statement = SqlRender.renderSql(sql_statement, new String[]{"drugID","conditionID","evidenceGroup","tableQualifier"},
	            new String[]{String.valueOf(drugID), String.valueOf(conditionID), evidenceType, tableQualifier});
	  sql_statement = SqlTranslate.translateSql(sql_statement, "sql server", source.getSourceDialect());
	  final List<EvidenceSummary> evidences = new ArrayList<EvidenceSummary>();
	  List<Map<String, Object>> rows = getSourceJdbcTemplate(source).queryForList(sql_statement);
	  
	  for (Map rs : rows) {	
	      EvidenceSummary e = new EvidenceSummary();
	      e.evidence_group_name = evidenceGroup;
	      //e.evidence_id = BigInteger.valueOf((long)rs.get("id"));
	      e.evidence_type = String.valueOf(rs.get("evidence_type"));
	      //e.supports = (Character)rs.get("supports");
	      e.evidence_count = Double.valueOf(String.valueOf(rs.get("statistic_value")));
	      
	      evidences.add(e);
	    }
	  return evidences;
  }
  
  
  /**
   * @param conditionID
   * @param drugID
   * @param evidenceType
   * @return
   */
  @GET
  @Path("{sourceKey}/evidencedetails")
  @Produces(MediaType.APPLICATION_JSON)
  public Collection<EvidenceDetails> getEvidenceDetails(@PathParam("sourceKey") String sourceKey, @QueryParam("conditionID") String conditionID, @QueryParam("drugID") String drugID, @QueryParam("evidenceType") String evidenceType) throws JSONException, IOException {
	  SparqlService sparqlentity = new SparqlService();
	  Source dbsource = getSourceRepository().findBySourceKey(sourceKey);
	  String tableQualifier = dbsource.getTableQualifier(SourceDaimon.DaimonType.Evidence);
	  String sql_statement = ResourceHelper.GetResourceAsString("/resources/evidence/sql/getEvidenceDetails.sql");
	  sql_statement = SqlRender.renderSql(sql_statement, new String[]{"drugID","conditionID","evidenceType","tableQualifier"},
	            new String[]{String.valueOf(drugID), String.valueOf(conditionID),String.valueOf(evidenceType), tableQualifier});
	  sql_statement = SqlTranslate.translateSql(sql_statement, "sql server", dbsource.getSourceDialect());
	  
	  final List<EvidenceDetails> evidences = new ArrayList<EvidenceDetails>();
	  List<Map<String, Object>> rows = getSourceJdbcTemplate(dbsource).queryForList(sql_statement);
	  List<LinkoutData> infoOnLinkout = new ArrayList<LinkoutData>();
	  String linkoutlist = null;
	  String[] linkouts = null;
	  for (Map rs : rows) {
	    	
	      linkoutlist = String.valueOf(rs.get("evidence_linkouts"));
	      if(linkoutlist.contains("|")) {
	    	  linkouts = linkoutlist.split(Pattern.quote("|"));
	      }
	      if((!linkoutlist.contains("|"))&&linkoutlist!= null){
	    	  linkouts = new  String[] {linkoutlist};
	      }
	      
	      for(int i=0;i<linkouts.length;i++)
	      {
	  		  linkouts[i] = sparqlentity.expandUrl(linkouts[i]);
	  		  linkouts[i] = URIUtil.decode(linkouts[i]);
	  		  linkouts[i] = URIUtil.encodeQuery(linkouts[i]);
	  		  JSONArray lineItems = sparqlentity.readJSONFeed(linkouts[i]);
	  		  
	  		for (int j = 0; j < lineItems.length(); ++j) {
	  			EvidenceDetails e = new EvidenceDetails();
	  			if(linkouts[i].contains("mesh")) {
	  				e = getPubMedlinkout(lineItems,j);
	  		    }
	  			if(linkouts[i].contains("ADR")) {
	  				e = getADRlinkout(lineItems,j);
	  			}
	  		    if(linkouts[i].contains("semmed")) {
	  		    	e = getSemMedlinkout(lineItems,j);
	  		    }
		        evidences.add(e);
		    }
	      }
	    }
	  
	  return evidences;
  }

  @POST
  @Path("{sourceKey}/spontaneousreports")
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes(MediaType.APPLICATION_JSON)
  public Collection<SpontaneousReport> getSpontaneousReports(@PathParam("sourceKey") String sourceKey, EvidenceSearch search) throws JSONException, IOException {
	  Source dbsource = getSourceRepository().findBySourceKey(sourceKey);
	  String tableQualifier = dbsource.getTableQualifier(SourceDaimon.DaimonType.Evidence);
	  String sql_statement = ResourceHelper.GetResourceAsString("/resources/evidence/sql/getSpontaneousReports.sql");          
          String conditionConceptListForQuery = this.JoinArray(search.conditionConceptList);
          String ingredientConceptListForQuery = this.JoinArray(search.ingredientConceptList);

          sql_statement = SqlRender.renderSql(sql_statement, new String[]{"conditionConceptList","ingredientConceptList", "tableQualifier"},
	            new String[]{conditionConceptListForQuery, ingredientConceptListForQuery, tableQualifier});
	  sql_statement = SqlTranslate.translateSql(sql_statement, "sql server", dbsource.getSourceDialect());
	  
	  final List<SpontaneousReport> results = new ArrayList<SpontaneousReport>();
	  List<Map<String, Object>> rows = getSourceJdbcTemplate(dbsource).queryForList(sql_statement);
	  for (Map rs : rows) {	
	      SpontaneousReport e = new SpontaneousReport();
	      e.conditionConceptId = String.valueOf(rs.get("CONDITION_CONCEPT_ID"));
	      e.conditionConceptName = String.valueOf(rs.get("CONDITION_CONCEPT_NAME"));
	      e.ingredientConceptId = String.valueOf(rs.get("INGREDIENT_CONCEPT_ID"));
	      e.ingredientConceptName = String.valueOf(rs.get("INGREDIENT_CONCEPT_NAME"));
              e.reportCount = Integer.valueOf(String.valueOf(rs.get("AERS")));
	      e.prr = BigDecimal.valueOf(Double.valueOf(String.valueOf(rs.get("AERS_PRR_ORIGINAL"))));
	      
	      results.add(e);
	    }
	  
	  return results;
  }

  @POST
  @Path("{sourceKey}/evidencesearch")
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes(MediaType.APPLICATION_JSON)
  public Collection<EvidenceUniverse> evidenceSearch(@PathParam("sourceKey") String sourceKey, EvidenceSearch search) throws JSONException, IOException {
	  Source dbsource = getSourceRepository().findBySourceKey(sourceKey);
	  String tableQualifier = dbsource.getTableQualifier(SourceDaimon.DaimonType.Evidence);
	  String sql_statement = ResourceHelper.GetResourceAsString("/resources/evidence/sql/getEvidenceFromUniverse.sql");          
          String conditionConceptListForQuery = this.JoinArray(search.conditionConceptList);
          String ingredientConceptListForQuery = this.JoinArray(search.ingredientConceptList);
          String evidenceTypeListForQuery = this.JoinArray(search.evidenceTypeList);

          sql_statement = SqlRender.renderSql(sql_statement, new String[]{"conditionConceptList","ingredientConceptList","evidenceTypeList","tableQualifier"},
	            new String[]{conditionConceptListForQuery, ingredientConceptListForQuery, evidenceTypeListForQuery, tableQualifier});
	  sql_statement = SqlTranslate.translateSql(sql_statement, "sql server", dbsource.getSourceDialect());
	  
	  final List<EvidenceUniverse> results = new ArrayList<EvidenceUniverse>();
	  List<Map<String, Object>> rows = getSourceJdbcTemplate(dbsource).queryForList(sql_statement);
	  for (Map rs : rows) {	
	      EvidenceUniverse e = new EvidenceUniverse();
              //e.evidence_id = Integer.valueOf(String.valueOf(rs.get("ID")));
	      e.condition_concept_id = Integer.valueOf(String.valueOf(rs.get("CONDITION_CONCEPT_ID")));
	      e.condition_concept_name = String.valueOf(rs.get("CONDITION_CONCEPT_NAME"));
	      e.ingredient_concept_id = Integer.valueOf(String.valueOf(rs.get("INGREDIENT_CONCEPT_ID")));
	      e.ingredient_concept_name = String.valueOf(rs.get("INGREDIENT_CONCEPT_NAME"));
              e.evidence_type = String.valueOf(rs.get("EVIDENCE_TYPE"));

	      if (rs.get("SUPPORTS") == null){
		  e.supports = new Character('u');
	      } else if (((String) rs.get("SUPPORTS")).equalsIgnoreCase("t")){
		  e.supports = new Character('t');
	      } else {
		  e.supports = new Character('f');
	      }
	      
              e.statistic_value = BigDecimal.valueOf(Double.valueOf(String.valueOf(rs.get("STATISTIC_VALUE"))));
              e.evidence_linkouts = String.valueOf(rs.get("EVIDENCE_LINKOUTS"));
	      
	      results.add(e);
	    }
	  
	  return results;
  }
  
  @POST
  @Path("{sourceKey}/labelevidence")
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes(MediaType.APPLICATION_JSON)
  public Collection<EvidenceUniverse> labelEvidence(@PathParam("sourceKey") String sourceKey, EvidenceSearch search) throws JSONException, IOException {
	  Source dbsource = getSourceRepository().findBySourceKey(sourceKey);
	  String tableQualifier = dbsource.getTableQualifier(SourceDaimon.DaimonType.Evidence);
	  String sql_statement = ResourceHelper.GetResourceAsString("/resources/evidence/sql/getLabelEvidence.sql");          
          String conditionConceptListForQuery = this.JoinArray(search.conditionConceptList);
          String ingredientConceptListForQuery = this.JoinArray(search.ingredientConceptList);
          String evidenceTypeListForQuery = this.JoinArray(search.evidenceTypeList);

          sql_statement = SqlRender.renderSql(sql_statement, new String[]{"conditionConceptList","ingredientConceptList","evidenceTypeList","tableQualifier"},
	            new String[]{conditionConceptListForQuery, ingredientConceptListForQuery, evidenceTypeListForQuery, tableQualifier});
	  sql_statement = SqlTranslate.translateSql(sql_statement, "sql server", dbsource.getSourceDialect());
	  
	  final List<EvidenceUniverse> results = new ArrayList<EvidenceUniverse>();
	  List<Map<String, Object>> rows = getSourceJdbcTemplate(dbsource).queryForList(sql_statement);
	  for (Map rs : rows) {	
	      EvidenceUniverse e = new EvidenceUniverse();
	      e.hasEvidence = String.valueOf((rs.get("Has_Evidence")));
	      e.ingredient_concept_id = Integer.valueOf(String.valueOf(rs.get("INGREDIENT_CONCEPT_ID")));
	      e.ingredient_concept_name = String.valueOf(rs.get("INGREDIENT_CONCEPT_NAME"));
              
	      results.add(e);
	    }
	  
	  return results;
  }
	
	
  @GET
  @Path("{sourceKey}/negativecontrols-sql")
  @Produces(MediaType.TEXT_PLAIN)
  @Consumes(MediaType.APPLICATION_JSON)
	public String getNegativeControlSql(@PathParam("sourceKey") String sourceKey) throws Exception {
		Long jobId = new Long(1);
		// Refactor to take this as a paramter
	  Source dbsource = getSourceRepository().findBySourceKey(sourceKey);
		
		// Create the task
		NegativeControl task = new NegativeControl();
		String outcomeOfInterest = "drug";
		String[] conceptsOfInterest = {"4344040"};
		String[] conceptsToExclude = {"0"};
		String[] conceptsToInclude = {"1186087","19015230","1381504","757688","1314865","715233","950933","1563600","980311","1541079","19008009","19010482","1311078","1304643","1338512","1301125","1151789","1352213","1304850","1542948","1597235","19048493","19097463","1536743","751889","19041065","787787","1512480","19117912","40238188","1112921","1351541","1192218","19024227","1305058","909841","708298","1114220","1522957","785788","1378382","19071160","19049105","753626","42903728","19090761","1584910","836208","1236744","40171288"};
		
		task.setSource(dbsource);
		task.setOutcomeOfInterest(outcomeOfInterest);
		task.setConceptsOfInterest(conceptsOfInterest);
		task.setConceptsToInclude(conceptsToInclude);
		task.setConceptsToExclude(conceptsToExclude);
		return getNegativeControlSqlNEW(task, jobId);
	}

/**
 * Queues up a negative control task, that generates and translates SQL for the
 * given concept ids
 *
 * @param task - the negative control task to run
 * @return information about the negative control job
 * @throws Exception
 */
  @POST
  @Path("{sourceKey}/negativecontrols")
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes(MediaType.APPLICATION_JSON)
  public JobExecutionResource queueNegativeControlsJob(@PathParam("sourceKey") String sourceKey, NegativeControl task) throws Exception {
        if (task == null) {
                return null;
        }
        JobParametersBuilder builder = new JobParametersBuilder();

        // Get a JDBC template for the OHDSI source repository 
        // and the source dialect for use when we write the results
        // back to the OHDSI repository
        JdbcTemplate jdbcTemplate = getJdbcTemplate();
        task.setJdbcTemplate(jdbcTemplate);
        String ohdsiDatasourceSourceDialect = getSourceDialect();
        task.setSourceDialect(ohdsiDatasourceSourceDialect);
        task.setOhdsiSchema(this.getOhdsiSchema());

        // source key comes from the client, we look it up here and hand it off to the tasklet
        Source source = getSourceRepository().findBySourceKey(sourceKey);
        task.setSource(source);

        if (!StringUtils.isEmpty(task.getJobName())) {
            builder.addString("jobName", limitJobParams(task.getJobName()));
        }
        builder.addString("concept_set_id", ("" + task.getConceptSetId()));
        builder.addString("concept_set_name", task.getConceptSetName());
        builder.addString("concept_domain_id", task.getConceptDomainId());
        builder.addString("source_id", ("" + source.getSourceId()));
        
        final String taskString = task.toString();
        final JobParameters jobParameters = builder.toJobParameters();
        log.info(String.format("Beginning run for negative controls analysis task: \n %s", taskString));

        NegativeControlTasklet tasklet = new NegativeControlTasklet(task, getSourceJdbcTemplate(task.getSource()), task.getJdbcTemplate(),
                        getTransactionTemplate(), this.conceptSetGenerationInfoRepository, this.getSourceDialect());

        return this.jobTemplate.launchTasklet("negativeControlsAnalysisJob", "negativeControlsAnalysisStep", tasklet, jobParameters);
}

  @GET
  @Path("{sourceKey}/negativecontrols/{conceptsetid}")
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes(MediaType.APPLICATION_JSON)
  public Collection<NegativeControlRecord> getNegativeControls(@PathParam("sourceKey") String sourceKey, @PathParam("conceptsetid") int conceptSetId) {
    Source source = getSourceRepository().findBySourceKey(sourceKey);
    return negativeControlRepository.findAllBySourceIdAndConceptId(source.getSourceId(), conceptSetId);
  }
	
	public static String getEvidenceJobIdSql(NegativeControl task) {
    String resourceRoot = "/resources/evidence/negativecontrols/";
    String sql = ResourceHelper.GetResourceAsString(resourceRoot + "getJobId.sql");
    String evidenceSchema = task.getSource().getTableQualifier(SourceDaimon.DaimonType.Evidence);
    String[] params = new String[]{"evidenceSchema"};
    String[] values = new String[]{evidenceSchema};
    sql = SqlRender.renderSql(sql, params, values);
    sql = SqlTranslate.translateSql(sql, task.getSource().getSourceDialect());

    return sql;
	}
 /**
  * Obtain the SQL that is used to retrieve the list of negative controls from the database
  * @param task
  * @return A sql statement
  */
  public static String getNegativeControlSql(NegativeControl task) {
    String resourceRoot = "/resources/evidence/sql/";
    String sql = ResourceHelper.GetResourceAsString(resourceRoot + "getNegativeControls.sql");
    if (task.getSource().getSourceDialect().equals("sql server")) {
        sql = ResourceHelper.GetResourceAsString(resourceRoot + "getNegativeControlsSqlServer.sql");
    }
    
    String tableQualifier = task.getSource().getTableQualifier(SourceDaimon.DaimonType.Evidence);
    String conceptIds = JoinArray(task.getConceptsOfInterest());
    String[] params = new String[]{"tableQualifier", "CONCEPT_IDS", "CONCEPT_SET_ID", "CONCEPT_SET_NAME", "CONCEPT_DOMAIN_ID", "TARGET_DOMAIN_ID", "SOURCE_ID"};
    String[] values = new String[]{tableQualifier, conceptIds, String.valueOf(task.getConceptSetId()), task.getConceptSetName(), task.getConceptDomainId().toUpperCase(), task.getOutcomeOfInterest().toUpperCase(), String.valueOf(task.getSource().getSourceId())};
    sql = SqlRender.renderSql(sql, params, values);
    sql = SqlTranslate.translateSql(sql, "sql server", task.getSource().getSourceDialect());

    return sql;
}
	
	public static String getNegativeControlSqlNEW(NegativeControl task, Long jobId ) {
		StringBuilder sb = new StringBuilder();
    String resourceRoot = "/resources/evidence/negativecontrols/";
		Source source = task.getSource();
	  String evidenceSchema = source.getTableQualifier(SourceDaimon.DaimonType.Evidence);
				
		/*
			################################################################################
			# CONFIG
			################################################################################
			outcomeOfInterest <- 'drug'
			conceptsOfInterest <- '4344040'
			conceptsToExclude <- '0'
			conceptsToInclude <- '1186087,19015230,1381504,757688,1314865,715233,950933,1563600,980311,1541079,19008009,19010482,1311078,1304643,1338512,1301125,1151789,1352213,1304850,1542948,1597235,19048493,19097463,1536743,751889,19041065,787787,1512480,19117912,40238188,1112921,1351541,1192218,19024227,1305058,909841,708298,1114220,1522957,785788,1378382,19071160,19049105,753626,42903728,19090761,1584910,836208,1236744,40171288'
		
		conceptsToExcludeData <- paste0(Sys.getenv("evidence"),".NC_EXCLUDED_CONCEPTS")
		conceptsToIncludeData <- paste0(Sys.getenv("evidence"),".NC_INCLUDED_CONCEPTS")
		indicationData <- paste0(Sys.getenv("evidence"),".NC_INDICATIONS")
		evidenceData <- paste0(Sys.getenv("evidence"),".NC_EVIDENCE")
		safeConceptData <- paste0(Sys.getenv("evidence"),".NC_SAFE_CONCEPTS")
		splicerConceptData <- paste0(Sys.getenv("evidence"),".NC_SPLICER_CONCEPTS")
		faersConceptsData <- paste0(Sys.getenv("evidence"),".NC_FAERS_CONCEPTS")
		adeSummaryData <- paste0(Sys.getenv("evidence"),".NC_ADE_SUMMARY")
		summaryData <- paste0(Sys.getenv("evidence"),".NC_SUMMARY")
		summaryOptimizedData <- paste0(Sys.getenv("evidence"),".NC_SUMMARY_OPTIMIZED")

		*/

		String outcomeOfInterest = task.getOutcomeOfInterest();
		String conceptsOfInterest = JoinArray(task.getConceptsOfInterest());
		String conceptsToExclude = JoinArray(task.getConceptsToExclude());
		String conceptsToInclude = JoinArray(task.getConceptsToInclude());
		// These are qualifiers for other schemas in CEM - verify w/ Erica
		String vocabularySchema = "vocabulary";
		String translatedSchema = "translated"; 
	  String faers = translatedSchema + ".AEOLUS";
		String splicer = translatedSchema + ".SPLICER";
		String ade = translatedSchema + ".MEDLINE_WINNENBURG";
		// These are temp tables 
		String conceptsToExcludeData = "#NC_EXCLUDED_CONCEPTS";
		String conceptsToIncludeData = "#NC_INCLUDED_CONCEPTS";
		String indicationData = "#NC_INDICATIONS";
		String evidenceData = "#NC_EVIDENCE";
		String safeConceptData = "#NC_SAFE_CONCEPTS";
		String splicerConceptData = "#NC_SPLICER_CONCEPTS";
		String faersConceptsData = "#NC_FAERS_CONCEPTS";
		String adeSummaryData = "#NC_ADE_SUMMARY";
		String summaryData = "#NC_SUMMARY";
		String summaryOptimizedData = "#NC_SUMMARY_OPTIMIZED";
		// These are pre-processed tables
		String conceptUniverseData = evidenceSchema + ".NC_LU_CONCEPT_UNIVERSE";
		String broadConceptsData = evidenceSchema + ".NC_LU_BROAD_CONDITIONS";
		String drugInducedConditionsData = evidenceSchema + ".NC_LU_DRUG_INDUCED_CONDITIONS";
		String pregnancyConditionData = evidenceSchema + ".NC_LU_PREGNANCY_CONDITIONS";
		// This is the results table
		String resultsData = evidenceSchema + ".NC_RESULTS";
	
		String[] params = new String[]{"outcomeOfInterest", "conceptsOfInterest", "vocabulary", "translated", "splicerData"};
    String[] values = new String[]{outcomeOfInterest, conceptsOfInterest, vocabularySchema, translatedSchema, splicer};
		
		/*
			################################################################################
			# FIND CONDITIONS OF INTEREST
			################################################################################

			#SPLICER
			findSplicerConcepts(conn=conn,
													storeData=splicerConceptData,
													splicerData=splicer,
													sqlFile="splicerConcepts.sql",
													conceptsOfInterest=conceptsOfInterest,
													vocabulary=vocabulary,
													outcomeOfInterest=outcomeOfInterest)
*/
		String sql = ResourceHelper.GetResourceAsString(resourceRoot + "splicerConcepts.sql");
    sql = SqlRender.renderSql(sql, params, values);
		sql = SqlRender.renderSql(sql, 
			new String[] {"storeData"}, 
			new String[] {splicerConceptData});
		sb.append(sql + "\n\n");

		/*
			#FIND INDICATIONS
			findDrugIndications(conn=conn,
													storeData=indicationData,
													vocabulary=vocabulary,
													conceptsOfInterest=conceptsOfInterest,
													outcomeOfInterest=outcomeOfInterest)
		*/
		
		sql = ResourceHelper.GetResourceAsString(resourceRoot + "findDrugIndications.sql");
    sql = SqlRender.renderSql(sql, params, values);
		sql = SqlRender.renderSql(sql, 
			new String[] {"storeData"}, 
			new String[] {indicationData});
		sb.append(sql + "\n\n");
		
		/*
			#USER IDENTIFIED CONCEPTS TO EXCLUDE
			findConcepts(conn = conn,
									 storeData = conceptsToExcludeData,
									 vocabulary=vocabulary,
									 concepts=conceptsToExclude,
									 expandConcepts=1)
		*/
		
		sql = ResourceHelper.GetResourceAsString(resourceRoot + "findConcepts.sql");
    sql = SqlRender.renderSql(sql, params, values);
		sql = SqlRender.renderSql(sql, 
			new String[] {"storeData", "concepts"}, 
			new String[] {conceptsToExcludeData, conceptsToExclude});
		sb.append(sql + "\n\n");
		
		/*
			#USER IDENTIFIED CONCEPTS TO INCLUDE
			findConcepts(conn = conn,
									 storeData = conceptsToIncludeData,
									 vocabulary=vocabulary,
									 concepts=conceptsToInclude)
		*/
		
		sql = ResourceHelper.GetResourceAsString(resourceRoot + "findConcepts.sql");
    sql = SqlRender.renderSql(sql, params, values);
		sql = SqlRender.renderSql(sql, 
			new String[] {"storeData", "concepts"}, new
				String[] {conceptsToIncludeData, conceptsToInclude});
		sb.append(sql + "\n\n");
		
		/*
			#FAERS
			findFaersADRs(conn = conn,
										faersData = faers,
										storeData = faersConceptsData,
										vocabulary=vocabulary,
										conceptsOfInterest=conceptsOfInterest,
										outcomeOfInterest = outcomeOfInterest)
		*/
		
		sql = ResourceHelper.GetResourceAsString(resourceRoot + "findFaersADRs.sql");
    sql = SqlRender.renderSql(sql, params, values);
		sql = SqlRender.renderSql(sql, 
			new String[] {"storeData", "faersData"}, 
			new String[] {faersConceptsData, faers});
		sb.append(sql + "\n\n");

		/*
			################################################################################
			# PULL EVIDENCE
			################################################################################
			pullEvidence(conn = conn,
										adeData = ade,
										storeData = adeSummaryData,
										vocabulary=vocabulary,
										conceptsOfInterest=conceptsOfInterest,
										outcomeOfInterest = outcomeOfInterest,
										conceptUniverse=conceptUniverseData)
		*/

		sql = ResourceHelper.GetResourceAsString(resourceRoot + "pullEvidence.sql");
    sql = SqlRender.renderSql(sql, params, values);
		sql = SqlRender.renderSql(sql, 
			new String[] {"storeData", "adeData", "conceptUniverseData"}, 
			new String[] {adeSummaryData, ade, conceptUniverseData});
		sb.append(sql + "\n\n");
		
		/*
			################################################################################
			# SUMMARIZE EVIDENCE
			################################################################################
			summarizeEvidence(conn=conn,
												outcomeOfInterest=outcomeOfInterest,
												conceptUniverseData=conceptUniverseData,
												storeData=summaryData,
												adeSummaryData=adeSummaryData,
												indicationData=indicationData,
												broadConceptsData=broadConceptsData,
												drugInducedConditionsData=drugInducedConditionsData,
												pregnancyConditionData=pregnancyConditionData,
												splicerConceptData=splicerConceptData,
												faersConceptsData=faersConceptsData,
												conceptsToExclude=conceptsToExcludeData,
												conceptsToInclude=conceptsToIncludeData)
		*/
		
		sql = ResourceHelper.GetResourceAsString(resourceRoot + "summarizeEvidence.sql");
    sql = SqlRender.renderSql(sql, params, values);
		sql = SqlRender.renderSql(sql, 
			new String[] {"storeData", "conceptUniverseData", "adeSummaryData", "indicationData", "broadConceptsData", "drugInducedConditionsData", "pregnancyConditionData", "splicerConceptData", "faersConceptsData", "conceptsToExclude", "conceptsToInclude"}, 
			new String[] {summaryData, conceptUniverseData, adeSummaryData, indicationData, broadConceptsData, drugInducedConditionsData, pregnancyConditionData, splicerConceptData, faersConceptsData, conceptsToExcludeData, conceptsToIncludeData});
		sb.append(sql + "\n\n");
		
		/*

			################################################################################
			# OPTIMIZE
			################################################################################
			optimizeEvidence(conn=conn,
											 outcomeOfInterest=outcomeOfInterest,
											 storeData=summaryOptimizedData,
											 vocabulary=vocabulary,
											 summaryData=summaryData)

			*/
		
		sql = ResourceHelper.GetResourceAsString(resourceRoot + "optimizeEvidence.sql");
    sql = SqlRender.renderSql(sql, params, values);
		sql = SqlRender.renderSql(sql, 
			new String[] {"storeData", "summaryData"}, 
			new String[] {summaryOptimizedData, summaryData});
		sb.append(sql + "\n\n");

	/*
		################################################################################
		# EXPORT
		################################################################################
		export(conn = conn,
					 file=fileName,
					 vocabulary = vocabulary,
					 outcomeOfInterest=outcomeOfInterest,
					 conceptsOfInterest = conceptsOfInterest,
					 conceptsToExcludeData = conceptsToExcludeData,
					 conceptsToIncludeData = conceptsToIncludeData,
					 summaryData = summaryData,
					 summaryOptimizedData = summaryOptimizedData,
					 adeSummaryData = adeSummaryData)

		*/		
		
		sql = ResourceHelper.GetResourceAsString(resourceRoot + "deleteJobResults.sql");
    sql = SqlRender.renderSql(sql, params, values);
		sql = SqlRender.renderSql(sql, 
			new String[] {"resultsData", "jobId"}, 
			new String[] {resultsData, Long.toString(jobId)});
		sb.append(sql + "\n\n");
		
		sql = ResourceHelper.GetResourceAsString(resourceRoot + "exportNegativeControls.sql");
    sql = SqlRender.renderSql(sql, params, values);
		sql = SqlRender.renderSql(sql, 
			new String[] {"resultsData", "jobId", "summaryOptimizedData", "summaryData"}, 
			new String[] {resultsData, Long.toString(jobId), summaryOptimizedData, summaryData});
		sb.append(sql + "\n\n");
		
		/*
    String sql = ResourceHelper.GetResourceAsString(resourceRoot + "getNegativeControls.sql");

		
    String evidenceSchema = task.getSource().getTableQualifier(SourceDaimon.DaimonType.Evidence);
    String conceptIds = JoinArray(task.getConceptIds());
    String[] params = new String[]{"evidenceSchema", "CONCEPT_IDS", "CONCEPT_SET_ID", "CONCEPT_SET_NAME", "CONCEPT_DOMAIN_ID", "TARGET_DOMAIN_ID", "SOURCE_ID"};
    String[] values = new String[]{evidenceSchema, conceptIds, String.valueOf(task.getConceptSetId()), task.getConceptSetName(), task.getConceptDomainId().toUpperCase(), task.getTargetDomainId().toUpperCase(), String.valueOf(task.getSource().getSourceId())};
    sql = SqlRender.renderSql(sql, params, values);
    sql = SqlTranslate.translateSql(sql, "sql server", task.getSource().getSourceDialect());

    return sql;
		*/
		//log.debug("OHDSql: \n\t" + sb.toString());
		sql = SqlTranslate.translateSql(sb.toString(), source.getSourceDialect());

		return sql;
	}

  public static String getNegativeControlDeleteStatementSql(NegativeControl task){
    String sql = ResourceHelper.GetResourceAsString("/resources/evidence/sql/deleteNegativeControls.sql");  
    sql = SqlRender.renderSql(sql, new String[] { "ohdsiSchema" },  new String[] { task.getOhdsiSchema() });
    sql = SqlTranslate.translateSql(sql, "sql server", task.getSourceDialect());

    return sql;            
}

  public static String getNegativeControlInsertStatementSql(NegativeControl task){
    String sql = ResourceHelper.GetResourceAsString("/resources/evidence/sql/insertNegativeControls.sql");
    sql = SqlRender.renderSql(sql, new String[] { "ohdsiSchema" },  new String[] { task.getOhdsiSchema() });
    sql = SqlTranslate.translateSql(sql, "sql server", task.getSourceDialect());

    return sql;            
}

  //parse ADRAnnotation linkouts
  private EvidenceDetails getADRlinkout(JSONArray lineItems,int j) throws JSONException {
	  EvidenceDetails e = new EvidenceDetails();
	  JSONObject tempItem = lineItems.getJSONObject(j);
      JSONObject tempSource;
      if(tempItem.has("an")) {
    	  tempSource = tempItem.getJSONObject("an");
    	  e.label = tempSource.getString("value");
      }
      if(tempItem.has("body")) {
    	  tempSource = tempItem.getJSONObject("body");
    	  e.bodyLabel = tempSource.getString("value");
      }
      if(tempItem.has("target")) {
    	  tempSource = tempItem.getJSONObject("target");
    	  e.target = tempSource.getString("value");
      }
      if(tempItem.has("sourceURL")) {
    	  tempSource = tempItem.getJSONObject("sourceURL");
    	  e.sourceURL = tempSource.getString("value");
      }
      if(tempItem.has("selector")) {
    	  tempSource = tempItem.getJSONObject("selector");
    	  e.selector = tempSource.getString("value");
      }
      if(tempItem.has("spl")) {
    	  tempSource = tempItem.getJSONObject("spl");
    	  e.splSection = tempSource.getString("value");
      }
      if(tempItem.has("text")) {
    	  tempSource = tempItem.getJSONObject("text");
    	  e.text = tempSource.getString("value");
      }
	  return e;
  }
  
  //parse Mesh linkouts
  private EvidenceDetails getPubMedlinkout(JSONArray lineItems,int j) throws JSONException {
	  EvidenceDetails e = new EvidenceDetails();
	  JSONObject tempItem = lineItems.getJSONObject(j);
      JSONObject tempSource;
      if(tempItem.has("an")) {
    	  tempSource = tempItem.getJSONObject("an");
    	  e.label = tempSource.getString("value");
      }
      if(tempItem.has("source")) {
    	  tempSource = tempItem.getJSONObject("source");
    	  e.sourceURL = tempSource.getString("value");
      }
      if(tempItem.has("pmid")) {
    	  tempSource = tempItem.getJSONObject("pmid");
    	  e.sourceURL = tempSource.getString("value");
      }

    return e;
  }
  
  //parse SemMed linkouts
  private EvidenceDetails getSemMedlinkout(JSONArray lineItems,int j) throws JSONException {
	  EvidenceDetails e = new EvidenceDetails();
	  JSONObject tempItem = lineItems.getJSONObject(j);
      JSONObject tempSource;
      if(tempItem.has("predicateLab")) {
    	  tempSource= tempItem.getJSONObject("predicateLab");
    	  e.predicateLabel = tempSource.getString("value");
      }
      if(tempItem.has("pmid")) {
    	  tempSource = tempItem.getJSONObject("pmid");
    	  e.sourceURL = tempSource.getString("value");
      }
      if(tempItem.has("studyType")) {
    	  tempSource = tempItem.getJSONObject("studyType");
    	  e.studyType = tempSource.getString("value");
      }
      if(tempItem.has("exact")) {
    	  tempSource = tempItem.getJSONObject("exact");
    	  e.exact = tempSource.getString("value");
      }
      if(tempItem.has("prefix")) {
    	  tempSource = tempItem.getJSONObject("prefix");
    	  e.prefix = tempSource.getString("value");
      }
      if(tempItem.has("postfix")) {
    	  tempSource = tempItem.getJSONObject("postfix");
    	  e.postfix = tempSource.getString("value");
      }
	  return e;
  }
  
  private static String JoinArray(final String[] array) {
    String result = "";

    for (int i = 0; i < array.length; i++) {
      if (i > 0) {
        result += ",";
      }

      result += "'" + array[i] + "'";
    }

    return result;
  }

  private static String limitJobParams(String param) {
            if (param.length() >= 250) {
                    return param.substring(0, 245) + "...";
            }
            return param;
    }
}
