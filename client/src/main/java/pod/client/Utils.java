package pod.client;

import java.util.stream.Stream;

public abstract class Utils {

    public static String parseParameter (String[] args, String paramToFind){
        return Stream.of(args).filter(arg -> arg.contains(paramToFind))
                .map(arg -> arg.substring(arg.indexOf("=")+ 1))
                .findFirst().orElseThrow(() -> new IllegalArgumentException(
                        "Must provide " + paramToFind + "=<value> param")
                );
    }


}