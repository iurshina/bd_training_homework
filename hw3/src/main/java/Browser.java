/**
 * Enum for counter to get stats of how many users of IE, Mozilla or Other were detected.
 *
 * @author Anastasiia_Iurshina
 */
public enum Browser {
    MOZILLA("mozilla"),
    SAFARI("safari"),
    OPERA("opera"),
    OTHERS("");

    private String name;

    Browser(final String name) {
        this.name = name;
    }

    public static Browser getCounter(final String[] tokens) {
        if (tokens.length < 12) {
            return Browser.OTHERS;
        }

        String token = tokens[11];
        if (tokens[11] == null) {
            return Browser.OTHERS;
        }

        token = token.toLowerCase();

        if (token.contains(Browser.MOZILLA.name)) {
            return Browser.MOZILLA;
        } else if (token.contains(Browser.OPERA.name)) {
            return Browser.OPERA;
        } else if (token.contains(Browser.SAFARI.name)) {
            return Browser.SAFARI;
        }

        return Browser.OTHERS;
    }
}
