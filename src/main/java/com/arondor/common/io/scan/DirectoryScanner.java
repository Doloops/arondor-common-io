package com.arondor.common.io.scan;

import java.io.File;
import java.io.FilenameFilter;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.log4j.Logger;

import com.arondor.common.io.AsyncIterator;


public class DirectoryScanner extends AsyncIterator<String> implements FileScanner
{
  private static final Logger log = Logger.getLogger(DirectoryScanner.class);


  /**
   * Check if a path has wildcard characters or not
   * @param path the path to check
   * @return true if the path has wildcards, false otherwise
   */
  public static boolean isWildcard(String path )
  {
    return ( path.contains("*") || path.contains("?"));
  }


  protected List<String> filters;
  
  
  public void setFilters(List<String> filters)
  {
    this.filters = filters;
  }

  
  protected boolean doScanOneItem()
  {
    buildList(filters);
    return false;
  }


  private void buildList(List<String> paths)
  {
    int total = paths.size();
    int nb = 0;
    for ( String path: paths )
    {
      buildList(path);
      nb++;
      log.debug ( "At path '" + path + "', (" + nb + " of " + total + ").");
    }
  }
  
  private void buildList(String wildcard)
  {
    /*
     * Simple optimization : if it's not a wildcard, it's fully-defined.
     */
    if ( ! isWildcard(wildcard) )
    {
      File resolvedFile = new File(wildcard);
      if ( resolvedFile.exists() )
      {
        log.debug("Found fully-resolved file : '" + resolvedFile.getAbsolutePath() + "'");
        addPath(resolvedFile.getAbsolutePath());
      }
      return;
    }
    wildcard = wildcard.replace('\\', '/');
    String wildcards[] = wildcard.split("/");
    File rootFolder = null;
    String rootFolderPath = "";
    if ( wildcard.startsWith("/") )
      rootFolderPath = "/";
    int startIdx;
    for ( startIdx = 0 ; startIdx < wildcards.length - 1 ; startIdx++ )
    {
      if ( isWildcard(wildcards[startIdx]) )
      {
        break;
      }
      rootFolderPath += wildcards[startIdx];
      rootFolderPath += "/";
    }
    log.debug("Found root (wildcard free) path : " + rootFolderPath );
    rootFolder = new File(rootFolderPath);
    if ( ! rootFolder.exists() )
    {
      log.debug("Path '" + rootFolderPath + "' does not exist !");
      return;
    }
    if ( ! rootFolder.isDirectory() )
    {
      throw new RuntimeException ( "Path '" + rootFolderPath + "' is not a folder !");
    }
    buildList(rootFolder, wildcards, startIdx);
  }
  
  private void buildList(File rootFolder, final String wildcards[], final int idx)
  {
    String wildcard = wildcards[idx];
    String regexSrc = wildcard.substring(wildcard.lastIndexOf("/") + 1);
    regexSrc = regexSrc.replace(".", "\\.");
    regexSrc = regexSrc.replace("*", ".*");
    regexSrc = regexSrc.replace("?", ".");
    
    final String regex = regexSrc.toLowerCase();
    /*
     * Marks the recursion flag '**'
     */
    final boolean recursiveRegex = regex.equals(".*.*");     
    
    log.debug("wildcard=" + wildcard + ", regex=" + regex);
    
    rootFolder.list(new FilenameFilter()
    {
      
      public boolean accept(File dir, String name)
      {
        log.debug("At dir : " + dir.getAbsolutePath() + ", name : " + name);
        if ( recursiveRegex || Pattern.matches(regex, name.toLowerCase()) )
        {
          log.debug("Matches : " + name);
          String absolutePath = dir.getAbsolutePath() + File.separator + name;
          File file = new File(absolutePath);
          if ( file.isDirectory() )
          {
            if ( idx < wildcards.length - 1 )
            {
              try
              {
                if ( recursiveRegex )
                {
                  buildList(file, wildcards, idx);
                }
                buildList(file, wildcards, idx+1);
              }
              catch (Exception e)
              {
                log.error("Could not fetch '" + absolutePath + "'", e);
              }
            }
          }
          else if ( ! recursiveRegex )
          {
            addPath(absolutePath);
          }
        }
        return false;
      }
    });
  }

  private void addPath(String absolutePath)
  {
    addObject(absolutePath);
  }
  
  
  public List<String> getIncludedFiles()
  {
    List<String> list = new ArrayList<String>();
    while ( hasNext() )
    {
      String file = next();
      list.add(file);
    }
    return list;
  }


}
