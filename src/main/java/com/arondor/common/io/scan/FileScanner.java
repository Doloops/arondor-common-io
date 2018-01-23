package com.arondor.common.io.scan;

import java.util.Iterator;
import java.util.List;

/**
 * Optimized File Scanner
 * 
 * @author Francois Barre
 *
 */
public interface FileScanner extends Iterator<String>
{
    public void setFilters(List<String> filters);

    public void setAsync(boolean async);

    public boolean isAsync();

    public List<String> getIncludedFiles();
}
