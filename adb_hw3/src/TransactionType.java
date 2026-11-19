public enum TransactionType {
    POINT_READ,
    INSERT,
    FULL_SCAN,
    WRITE_UPDATE;

    /**
     * Dices a transaction type according to the Homework 3 probability distribution:
     * - p < 0.6 : Read-only multi-point query (60%)
     * - 0.6 <= p < 0.7 : Insert a new tuple (10%)
     * - 0.7 <= p < 0.8 : Full table read-only scan (10%)
     * - p >= 0.8 : Write query with 3 single-tuple updates (20%)
     */
    public static TransactionType dice(double p) {
        if (p < 0.6) {
            return POINT_READ;
        } else if (p < 0.7) {
            return INSERT;
        } else if (p < 0.8) {
            return FULL_SCAN;
        } else {
            return WRITE_UPDATE;
        }
    }
}
