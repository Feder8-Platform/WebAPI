package org.ohdsi.webapi.service;

import java.util.*;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import org.ohdsi.webapi.DataSourceLookup;
import org.ohdsi.webapi.source.Source;
import org.ohdsi.webapi.source.SourceDaimon;
import org.ohdsi.webapi.source.SourceInfo;
import org.ohdsi.webapi.source.SourceRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Path("/source/")
@Component
public class SourceService extends AbstractDaoService {

  public class SortByKey implements Comparator<SourceInfo>
  {
    private boolean isAscending;
    
    public SortByKey(boolean ascending) {
      isAscending = ascending;      
    }
    
    public SortByKey() {
      this(true);
    }
    
    public int compare(SourceInfo s1, SourceInfo s2) {
      return s1.sourceKey.compareTo(s2.sourceKey) * (isAscending ? 1 : -1);
    }    
  }
  @Autowired
  private SourceRepository sourceRepository;

  @Autowired
  private DataSourceLookup dataSourceLookup;

  private static Collection<SourceInfo> cachedSources = null;
  
  @Path("sources")
  @GET
  @Produces(MediaType.APPLICATION_JSON)
  public Collection<SourceInfo> getSources() {

    if (cachedSources == null) {
      Iterable<Source> sourceIterable = sourceRepository.findAll();
      List<SourceInfo> sources = new ArrayList<>();
      for (Source source : sourceIterable) {
        sources.add(new SourceInfo(source));
      }
      Collections.sort(sources, new SortByKey());
      cachedSources = sources;
      initDataSources(sourceIterable);
    }
    return cachedSources;
  }
  
  @Path("refresh")
  @GET
  @Produces(MediaType.APPLICATION_JSON)  
  public Collection<SourceInfo> refreshSources() {
    cachedSources = null;
    return getSources();
  }  

  @Path("priorityVocabulary")
  @GET
  @Produces(MediaType.APPLICATION_JSON)
  public SourceInfo getPriorityVocabularySourceInfo() {
    int priority = 0;
    SourceInfo priorityVocabularySourceInfo = null;

    for (Source source : sourceRepository.findAll()) {
      for (SourceDaimon daimon : source.getDaimons()) {
        if (daimon.getDaimonType() == SourceDaimon.DaimonType.Vocabulary) {
          int daimonPriority = Integer.parseInt(daimon.getPriority());
          if (daimonPriority >= priority) {
            priority = daimonPriority;
            priorityVocabularySourceInfo = new SourceInfo(source);
          }
        }
      }
    }

    return priorityVocabularySourceInfo;
  }

  @Path("{key}")
  @GET
  @Produces(MediaType.APPLICATION_JSON)
  public SourceInfo getSource(@PathParam("key") final String sourceKey) {
    return sourceRepository.findBySourceKey(sourceKey).getSourceInfo();
  }

  /**
   * Initialize the application data sources based on the collection of Source / SourceDaimons
   * @param sourceIterable iterable of sources (including their source daimons)
   */
  private void initDataSources(Iterable<Source> sourceIterable) {
    dataSourceLookup.initDataSources(sourceIterable);
  }
}
