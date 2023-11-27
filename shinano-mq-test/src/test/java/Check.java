import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.ansj.domain.Result;
import org.ansj.domain.Term;
import org.ansj.splitWord.analysis.DicAnalysis;
import org.ansj.splitWord.analysis.NlpAnalysis;
import org.junit.jupiter.api.Test;
import org.nlpcn.commons.lang.tire.domain.Forest;
import org.nlpcn.commons.lang.tire.library.Library;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author lhe.shinano
 * @date 2023/11/20
 */
public class Check {
    Pattern MMLU_Pattern = Pattern.compile("(\\([a-g]|[A-G]\\))+");
    Pattern MMLU_PATTERN_2 = Pattern.compile("is ([a-g]|[A-G])");

    @Test
    public void testPattern() {
        Matcher matcher = MMLU_PATTERN_2.matcher(
         " The correct answer is C. Humanism.\n" +
                 " \n" +
                 " The passage provided is from Baldassare Castiglione's Book of the Courtier, published in 1528. This book is a prime example of the humanist philosophy that was prevalent during the Renaissance era. The text emphasizes the importance of a well-rounded education, including mastery of various skills such as arms, languages, literature, and music. It also highlights the importance of personal qualities like strength, loyalty, and grace.\n" +
                 " \n" +
                 " Humanism was a cultural and intellectual movement that emerged during the Renaissance, focusing on the potential and achievements of human beings. It emphasized the importance of education, individualism, and the appreciation of classical culture. The passage provided reflects these ideals, making humanism the most connected theme to the given writing."
        );

        if (matcher.find()) {
            String group = matcher.group(0);
            String replace = group.replace("is", "").replace(" ", "");
            System.out.println(replace);
        }
    }
    @Test
    public void test() {

        try (BufferedReader br = new BufferedReader(new FileReader("C:\\Users\\lhe.shinano\\Desktop\\MMLU-ERROR.json"))){
            String line = "";
            StringBuilder sb = new StringBuilder();
            int count = 0;
            int total = 0;
            while ((line = br.readLine()) != null) {
                sb.append(line);
            }
            JSONArray array = JSONArray.parseArray(sb.toString());
            for (Object obj : array) {
                total++;
                JSONObject jb = (JSONObject) obj;

                String result = jb.getString("modelResult");
                String answer = jb.getString("truth");

                Matcher matcher = MMLU_Pattern.matcher(result);
                if(matcher.find()) {
                    String group = matcher.group(0);
                    group = group.replace("(", "")
                            .replace(")", "");

                    if(answer.equalsIgnoreCase(group)) {
                        count++;
                        System.out.println(result+"---"+answer);
                        continue;
                    }
                }

                Matcher matcher1 = MMLU_PATTERN_2.matcher(result);
                if(matcher1.find()) {
                    String group = matcher1.group(0);
                    group = group.replace("is ", "")
                            .replace(" ", "");

                    if(answer.equalsIgnoreCase(group)) {
                        count++;
                        System.out.println(result+"---"+answer);
                        continue;
                    }
                }
            }
            System.out.println("正确数:"+count + "total:" + total);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
