package com.mongodb;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.Set;

/**
 * User: dervish
 * Date: 4/22/13
 * Time: 4:49 PM
 *
 * This class is partially copied to override check() method that should
 * perform check before accessing connector field to prevent NPE.
 *
 */
public class FongoDBCursor extends DBCursor {
    public FongoDBCursor(FongoDBCollection fongoDBCollection, DBObject ref, DBObject o, ReadPreference readPreference) {
        super(fongoDBCollection,ref,o,readPreference);
        _collection = fongoDBCollection;
        _query = ref == null ? new BasicDBObject() : ref;
        _keysWanted = o;
        _options = _collection.getOptions();
        _readPref = readPreference;
        _decoderFact = fongoDBCollection.getDBDecoderFactory();
    }

    /**
     * Returns the object the cursor is at and moves the cursor ahead by one.
     * @return the next element
     * @throws MongoException
     */
    public DBObject next() {
        _checkType( CursorType.ITERATOR );
        return _next();
    }

    private DBObject _next() {
        if ( _cursorType == null )
            _checkType( CursorType.ITERATOR );

        _check();

        _cur = _it.next();
        _num++;

        if ( _keysWanted != null && _keysWanted.keySet().size() > 0 ){
            _cur.markAsPartialObject();
            //throw new UnsupportedOperationException( "need to figure out partial" );
        }

        if ( _cursorType == CursorType.ARRAY ){
            _all.add( _cur );
        }

        return _cur;
    }


    /**
     * Checks if there is another object available
     * @return
     * @throws MongoException
     */
    public boolean hasNext() {
        _checkType( CursorType.ITERATOR );
        return _hasNext();
    }

    private boolean _hasNext() {
        _check();

        if ( this._limit > 0 && _num >= _limit )
            return false;

        return _it.hasNext();
    }

    private void _check() {
        if (_it != null)
            return;

        _lookForHints();

        QueryOpBuilder builder = new QueryOpBuilder()
                .addQuery(_query)
                .addOrderBy(_orderBy)
                .addHint(_hintDBObj)
                .addHint(_hint)
                .addExplain(_explain)
                .addSnapshot(_snapshot)
                .addSpecialFields(_specialFields);

        if (_collection.getDB().getMongo().getConnector() != null && _collection.getDB().getMongo().isMongosConnection()) {
            builder.addReadPreference(_readPref.toDBObject());
        }

        _it = _collection.__find(builder.get(), _keysWanted, _skip, _batchSize, _limit,
                _options, _readPref, getDecoder());
    }

    // Only create a new decoder if there is a decoder factory explicitly set on the collection.  Otherwise return null
    // so that the collection can use a cached decoder
    private DBDecoder getDecoder() {
        return _decoderFact != null ? _decoderFact.create() : null;
    }

    /**
     * if there is a hint to use, use it
     */
    private void _lookForHints(){

        if ( _hint != null ) // if someone set a hint, then don't do this
            return;

        if ( _collection._hintFields == null )
            return;

        Set<String> mykeys = _query.keySet();

        for ( DBObject o : _collection._hintFields ){

            Set<String> hintKeys = o.keySet();

            if ( ! mykeys.containsAll( hintKeys ) )
                continue;

            hint( o );
            return;
        }
    }

    /**
     * Creates a copy of an existing database cursor.
     * The new cursor is an iterator, even if the original
     * was an array.
     *
     * @return the new cursor
     */
    public DBCursor copy() {
        DBCursor c = new FongoDBCursor(_collection, _query, _keysWanted, _readPref);
        return c;
    }

    private final FongoDBCollection _collection;
    private final DBObject _query;
    private final DBObject _keysWanted;

    private DBObject _orderBy = null;
    private String _hint = null;
    private DBObject _hintDBObj = null;
    private boolean _explain = false;
    private int _limit = 0;
    private int _batchSize = 0;
    private int _skip = 0;
    private boolean _snapshot = false;
    private int _options = 0;
    private ReadPreference _readPref;
    private DBDecoderFactory _decoderFact;
    private DBObject _specialFields;

    // ----  result info ----
    private Iterator<DBObject> _it = null;

    private CursorType _cursorType = null;
    private DBObject _cur = null;
    private int _num = 0;
    private final ArrayList<DBObject> _all = new ArrayList<DBObject>();

}
