package com.learnkafka.launcher;

import com.learnkafka.producer.MessageProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import static com.learnkafka.producer.MessageProducer.propsMap;

public class CommandLineLauncher {
    private static final Logger logger = LoggerFactory.getLogger(MessageProducer.class);

    public static String commandLineStartLogo(){

        return "Welcome to command line producer";
    }

    public static String BYE(){

        return "Bye";
    }

    public static void launchCommandLine(){
        boolean cliUp = true;
        while (cliUp){
            Scanner scanner = new Scanner(System.in);
            userOptions();
            String option = scanner.next();
            logger.info("Selected Option is : {} ", option);
            switch (option) {
                case "1":
                    acceptMessageFromUser(option);
                    break;
                case "2":
                    cliUp = false;
                    break;
                default:
                    break;

            }
        }
    }

    public static void userOptions(){
        List<String> userInputList = new ArrayList<>();
        userInputList.add("1: Kafka Producer");
        userInputList.add("2: Exit");
        System.out.println("Please select one of the below options:");
        for(String userInput: userInputList ){
            System.out.println(userInput);
        }
    }
    public static MessageProducer init(){

        Map<String, Object> producerProps = propsMap();
        MessageProducer messageProducer = new MessageProducer(producerProps);
        return messageProducer;
    }

    public static void publishMessage(MessageProducer messageProducer, String input){
        StringTokenizer stringTokenizer = new StringTokenizer(input, "-");
        Integer noOfTokens = stringTokenizer.countTokens();
        switch (noOfTokens){
            case 1:
                messageProducer.publishMessageSync(null,stringTokenizer.nextToken());
                break;
            case 2:
                messageProducer.publishMessageSync(stringTokenizer.nextToken(),stringTokenizer.nextToken());
                break;
            default:
                break;
        }
    }

    public static void acceptMessageFromUser(String option){
        Scanner scanner = new Scanner(System.in);
        boolean flag= true;
        while (flag){
            System.out.println("Please Enter a Message to produce to Kafka:");
            String input = scanner.nextLine();
            logger.info("Entered message is {}", input);
            if(input.equals("00")) {
                flag = false;
            }else {
                MessageProducer messageProducer = init();
                publishMessage(messageProducer, input);
                messageProducer.close();
            }
        }
        logger.info("Exiting from Option : " + option);
    }

    public static void main(String[] args) {

        System.out.println(commandLineStartLogo());
        launchCommandLine();
        System.out.println(BYE());


    }
}
