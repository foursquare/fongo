package com.mongodb;

public class FongoWriteResult extends WriteResult {
    /**
     * Number of documents affected by a write operation.
     */
    private int n;

    FongoWriteResult(CommandResult o, WriteConcern concern, int n) {
        super(o, concern);
        this.n = n;
    }

    FongoWriteResult(DB db, DBPort p, WriteConcern concern, int n) {
        super(db, p, concern);
        this.n = n;
    }

    @Override
    public int getN() {
        return n;
    }
}
