import java.util.*;
import java.io.*;
import java.nio.file.Paths;
import java.nio.file.Files;
class NaiveWordCount {
    static HashMap<String, Integer> wordCount = new HashMap<String, Integer>();
    public static void main(String [] args) {
        long start = System.currentTimeMillis();
        countWordsUsingStringTokenizer("mobydick.txt");
        countWordsUsingStringTokenizer("mobydick.txt");
        countWordsUsingStringTokenizer("mobydick.txt");
        writeMapToFile();
        long end = System.currentTimeMillis();
        System.out.println("Time Taken: " + (end - start) / 1000F);
    }
    public static void countWordsUsingStringTokenizer(String filePath) {
        try (BufferedReader br = Files.newBufferedReader(Paths.get(filePath))) {
            String line;
            System.out.println("Processing file");
            while ((line = br.readLine()) != null) {
                // process the line.
                String[] words = line.split(" ");
                for(int i = 0; i < words.length; i++) {
                    String word = words[i].replaceAll("\\p{Punct}", "");
                    if(!wordCount.containsKey(word)) {
                        wordCount.put(word, 0);
                    }
                    wordCount.put(word, wordCount.get(word) + 1);
                }
                        
            }
        }catch (IOException e) {
            System.out.println(e);
        }
  }
  public static void writeMapToFile() {
    try (BufferedWriter writer = new BufferedWriter(new FileWriter("naive_output.txt"))) {
        for (String key : wordCount.keySet()) {
                String line = key + ": " + wordCount.get(key) + "\n";
                writer.write(line); 
            }
        writer.close();
    } catch (IOException e) {
        System.out.println(e);
    }
  }
}