package com.chengzw;

import org.apache.commons.cli.*;

/**
 * @author chengzw
 * @description 运行：java -classpath commons_cli-1.0-SNAPSHOT-jar-with-dependencies.jar com.chengzw.CLITester -a 1 2 3 4 5
 * @since 2021/9/4
 */
public class CLITester {
    public static void main(String[] args) throws ParseException, ParseException {
        //***Definition Stage***
        // create Options object
        Options options = new Options();
        // add option "-a"
        options.addOption("a", false, "add numbers");
        // add option "-m"
        options.addOption("m", false, "multiply numbers");
        //***Parsing Stage***
        //Create a parser
        CommandLineParser parser = new DefaultParser();
        //parse the options passed as command line arguments
        CommandLine cmd = parser.parse( options, args);
        //***Interrogation Stage***
        //hasOptions checks if option is present or not
        if(cmd.hasOption("a")) {
            System.out.println("Sum of the numbers: " + getSum(args));
        } else if(cmd.hasOption("m")) {
            System.out.println("Multiplication of the numbers: " + getMultiplication(args));
        }
    }
    public static int getSum(String[] args) {
        int sum = 0;
        for(int i = 1; i < args.length ; i++) {
            sum += Integer.parseInt(args[i]);
        }
        return sum;
    }
    public static int getMultiplication(String[] args) {
        int multiplication = 1;
        for(int i = 1; i < args.length ; i++) {
            multiplication *= Integer.parseInt(args[i]);
        }
        return multiplication;
    }
}