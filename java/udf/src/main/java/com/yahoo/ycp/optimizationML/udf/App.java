package com.yahoo.ycp.optimizationML.udf;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.hsqldb.lib.Iterator;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args )
    {
        String ans = new App().minWindow("a", "a");
        System.out.println(ans);
    }
    
    private static void testIter(List<String> ls) {
    	System.out.println(ls.get(0) + ":>");
    	System.out.println(ls.get(1) + ":>");
    	System.out.println(ls.get(2) + ":>");
    }
    
    public String minWindow(String s, String t) {
        if (s.length() == 0 || t.length() == 0 )
            return "";
        Set<Character> set = new HashSet<Character>();
        for (char c : t.toCharArray()) {
            set.add(c);
        }
        int i = 0;
        int j = 0;
        while( i < s.length() ) {
            if (set.contains(s.charAt(i)) )
                break;
            i++;
        }
        if (i >= s.length() )
            return "";
        HashMap<Character, Integer> hm = new HashMap<Character, Integer>();

        for (j = i; j < s.length() && hm.size() < set.size() ; j++) {
            char c = s.charAt(j);
            if ( set.contains(j) ) {
                if (hm.containsKey(c) ) {
                    hm.put(c, hm.get(c)+1 );
                } else {
                    hm.put(c, 1);
                }
            }
        }
        if (hm.size() < set.size() )
            return "";
        int bestI = i;
        int bestJ = j-1;
        int bestLen = bestJ - bestI;
        for (; j < s.length(); j++ ) {
            char c = s.charAt(j);
            if ( set.contains(c) ) {
                hm.put(c, hm.get(c)+1 );
                while( !hm.containsKey(s.charAt(i)) || hm.get(s.charAt(i) ) > 1 ) {
                    i++;
                    if (hm.containsKey(s.charAt(i)) ){
                        hm.put(s.charAt(i), hm.get(s.charAt(i))-1 );
                    }
                }
            }
            int len = j-i+1;
            if (len < bestLen) {
                bestI = i;
                bestJ = j;
                bestLen = len;
            }
        }
        return s.substring(bestI, bestJ+1);
    }
}
