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

        String userAgent = tokens[11];
        if (userAgent == null) {
            return Browser.OTHERS;
        }

        userAgent = userAgent.toLowerCase();

        if (userAgent.contains(Browser.MOZILLA.name)) {
            return Browser.MOZILLA;
        } else if (userAgent.contains(Browser.OPERA.name)) {
            return Browser.OPERA;
        } else if (userAgent.contains(Browser.SAFARI.name)) {
            return Browser.SAFARI;
        }

        return Browser.OTHERS;
    }
}
