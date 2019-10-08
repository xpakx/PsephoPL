package io.github.xpakx.psephopl;

import io.github.xpakx.psephopl.utils.CSVUtils;
import java.util.List;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args )
    {
        List<List<String>> list = CSVUtils.readFile("2015-gl-lis-okr.csv");
        System.out.println( list );
    }
}
