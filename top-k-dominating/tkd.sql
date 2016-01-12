CREATE OR REPLACE FUNCTION tkd(text, int) RETURNS SETOF record
        AS '$libdir/tkd', 'native_tkd'
        LANGUAGE C STRICT;
