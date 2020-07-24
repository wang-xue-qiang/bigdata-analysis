package com.pusidun.utils;

import org.wltea.analyzer.core.IKSegmenter;
import org.wltea.analyzer.core.Lexeme;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

/**
 * IK分词工具
 */
public class IkUtils {

    /**
     * 切分单词
     * @param msg
     * @return
     */
    public static List<String> IkSplit(String msg) {
        List<String> list = new ArrayList<String>();
        try {
            StringReader sr = new StringReader(msg);
            IKSegmenter ik = new IKSegmenter(sr, true);
            Lexeme lex;
            while ((lex = ik.next()) != null) {
                list.add(lex.getLexemeText());
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return list;
    }

    public static void main(String[] args) {
        List<String> list = IkUtils.IkSplit("电脑平板电脑小米笔记本华为笔记本");
        for (String str : list) {
            System.out.println(str);
        }
    }

}
