DROP TYPE geocoord CASCADE;

CREATE FUNCTION gcoord_in(cstring)
   RETURNS geocoord
   AS '/localstorage/z5257072/assignment1/postgresql-15.1/src/tutorial/gcoord'
   LANGUAGE C IMMUTABLE STRICT;


CREATE FUNCTION gcoord_out(geocoord)
   RETURNS cstring
   AS '/localstorage/z5257072/assignment1/postgresql-15.1/src/tutorial/gcoord'
   LANGUAGE C IMMUTABLE STRICT;


CREATE TYPE geocoord (
   internallength = VARIABLE,
   input = gcoord_in,
   output = gcoord_out
);


CREATE FUNCTION gcoord_eq(geocoord, geocoord) RETURNS bool
   AS '/localstorage/z5257072/assignment1/postgresql-15.1/src/tutorial/gcoord' 
   LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION gcoord_neq(geocoord, geocoord) RETURNS bool
   AS '/localstorage/z5257072/assignment1/postgresql-15.1/src/tutorial/gcoord' 
   LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION gcoord_gt(geocoord, geocoord) RETURNS bool
   AS '/localstorage/z5257072/assignment1/postgresql-15.1/src/tutorial/gcoord' 
   LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION gcoord_lt(geocoord, geocoord) RETURNS bool
   AS '/localstorage/z5257072/assignment1/postgresql-15.1/src/tutorial/gcoord' 
   LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION gcoord_gt_or_eq(geocoord, geocoord) RETURNS bool
   AS '/localstorage/z5257072/assignment1/postgresql-15.1/src/tutorial/gcoord' 
   LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION gcoord_lt_or_eq(geocoord, geocoord) RETURNS bool
   AS '/localstorage/z5257072/assignment1/postgresql-15.1/src/tutorial/gcoord' 
   LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION gcoord_timezone_eq(geocoord, geocoord) RETURNS bool
   AS '/localstorage/z5257072/assignment1/postgresql-15.1/src/tutorial/gcoord' 
   LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION gcoord_timezone_neq(geocoord, geocoord) RETURNS bool
   AS '/localstorage/z5257072/assignment1/postgresql-15.1/src/tutorial/gcoord' 
   LANGUAGE C IMMUTABLE STRICT;


CREATE OPERATOR < (
   leftarg = geocoord,
   rightarg = geocoord,
   procedure = gcoord_lt,
   commutator = > , 
   negator = >= ,
   restrict = scalargtsel, 
   join = scalargtjoinsel
);

CREATE OPERATOR <= (
   leftarg = geocoord, 
   rightarg = geocoord, 
   procedure = gcoord_lt_or_eq,
   commutator = >= , 
   negator = > ,
   restrict = scalarlesel, 
   join = scalarlejoinsel
);

CREATE OPERATOR = (
   leftarg = geocoord,
   rightarg = geocoord,
   procedure = gcoord_eq,
   commutator = = ,
   negator = <> ,
   restrict = eqsel,
   join = eqjoinsel
);

CREATE OPERATOR <> (
   leftarg = geocoord, 
   rightarg = geocoord, 
   procedure = gcoord_neq,
   commutator = <> , 
   negator = = ,
   restrict = neqsel,
   join = neqjoinsel
);

CREATE OPERATOR >= (
   leftarg = geocoord, 
   rightarg = geocoord, 
   procedure = gcoord_gt_or_eq,
   commutator = <= , 
   negator = < ,
   restrict = scalargesel, 
   join = scalargejoinsel
);

CREATE OPERATOR > (
   leftarg = geocoord, 
   rightarg = geocoord, 
   procedure = gcoord_gt,
   commutator = < , 
   negator = <= ,
   restrict = scalargtsel, 
   join = scalargtjoinsel
);


CREATE OPERATOR ~ (
   leftarg = geocoord, 
   rightarg = geocoord, 
   procedure = gcoord_timezone_eq,
   commutator = ~ , 
   negator = !~ ,
   restrict = eqsel, 
   join = eqjoinsel
);

CREATE OPERATOR !~ (
   leftarg = geocoord, 
   rightarg = geocoord, 
   procedure = gcoord_timezone_neq,
   commutator = !~ , 
   negator = ~ ,
   restrict = neqsel,
   join  = neqjoinsel
);

CREATE FUNCTION convert2dms(geocoord)
   RETURNS cstring
   AS '/localstorage/z5257072/assignment1/postgresql-15.1/src/tutorial/gcoord'
   LANGUAGE C IMMUTABLE STRICT;


CREATE FUNCTION gcoord_cmp(geocoord, geocoord) RETURNS int4
   AS '/localstorage/z5257072/assignment1/postgresql-15.1/src/tutorial/gcoord' LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION gcoord_hval(geocoord) RETURNS int4
   AS '/localstorage/z5257072/assignment1/postgresql-15.1/src/tutorial/gcoord' LANGUAGE C IMMUTABLE STRICT;

CREATE OPERATOR CLASS gcoord_btree_ops
    DEFAULT FOR TYPE geocoord USING btree AS
        OPERATOR        1       < ,
        OPERATOR        2       <= ,
        OPERATOR        3       = ,
        OPERATOR        4       >= ,
        OPERATOR        5       > ,
        FUNCTION        1       gcoord_cmp(geocoord, geocoord);

CREATE OPERATOR CLASS gcoord_hash_ops
    DEFAULT FOR TYPE geocoord USING hash AS
        OPERATOR        1       = ,
        FUNCTION        1       gcoord_hval(geocoord);
