#include "postgres.h"

#include "fmgr.h"
#include "libpq/pqformat.h"		/* needed for send/recv functions */



#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <regex.h>
#include <ctype.h>
#include <math.h>
#include "access/hash.h"
#include <stdbool.h>

PG_MODULE_MAGIC;

typedef struct Coord
{
	double		degrees;
	char		direction;
}			Coord;		

typedef struct GeoCoord
{
	int 		length;
	Coord		Latitude;
	Coord		Longitude;
	char		Location[1];

}			GeoCoord;

int initCoords(char * c1, char *c2, GeoCoord * res);
int initLocation(char * location, GeoCoord ** res);
int regexMatch(char * str, char * regexPattern);
char *formatedLocationName(char * str);

/*****************************************************************************
 * helper functions
 *****************************************************************************/

int regexMatch(char * str, char * regexPattern) {
	regex_t regex;
	int match = 0;
	if(regcomp(&regex, regexPattern, REG_EXTENDED|REG_ICASE)) {
		return -1;
	}
	if(!regexec(&regex, str, 0, NULL, 0)) {
		match = 1;
	} 
	regfree(&regex);
	return match;
}

char *formatedLocationName(char *str) {

	char  *formattedName = palloc(strlen(str) + 1);

	strcpy(formattedName, str);
	formattedName[0] = toupper(str[0]);
	for(int i = 0; i < strlen(str); i++) {
		if(str[i] == ' ') {
			formattedName[i + 1] = toupper(str[i + 1]);
		}
	}

	return formattedName;
}

int initCoords(char * c1, char *c2, GeoCoord * res) {

	char 	*degressStrC1, *degressStrC2, *ptrC1, *ptrC2;
	char  	directionC1, directionC2;
	double 	degreesC1, degreesC2;
	
	degressStrC1 = strtok(c1, "°");
	directionC1 = strtok(NULL, "°")[0];

	degressStrC2 = strtok(c2, "°");
	directionC2 = strtok(NULL, "°")[0];

	if (directionC1 && directionC2) {

		degreesC1 = strtod(degressStrC1, &ptrC1);

		degreesC2 = strtod(degressStrC2, &ptrC2);

		if('N' == directionC1 || 'S' == directionC1) {
			 if('W' == directionC2 || 'E' == directionC2) {
				if(degreesC1 <= 90 && degreesC1 >= 0 && degreesC2 <= 180 && degreesC2 >= 0) {
					res->Latitude.direction = directionC1;
					res->Longitude.direction = directionC2;
					res->Latitude.degrees = degreesC1;
					res->Longitude.degrees = degreesC2;
					return 1;
				}
			 }
		}
		if('W' == directionC1 || 'E' == directionC1) {
			if('N' == directionC2 || 'S' == directionC2) {
				if(degreesC2 <= 90 && degreesC2 >= 0 && degreesC1 <= 180 && degreesC1 >= 0) {
					res->Latitude.direction = directionC2;
					res->Longitude.direction= directionC1;
					res->Latitude.degrees = degreesC2;
					res->Longitude.degrees = degreesC1;
					return 1;
				}
			 }
		}
	}
	return 0;
	
}

int initLocation(char * location, GeoCoord ** res) {

	char loc[256];
	char *c1, *c2, *regexA, *regexB, *locationName;
	int regexMatches, validCoords;
	
	regexMatches = 0;
	regexA = "^[a-zA-Z]+( [a-zA-Z]+)*,[0-9]+(.[0-9]+)?°[NESW]{1},[0-9]+(.[0-9]+)?°[NESW]{1}$";
	regexB = "^[a-zA-Z]+( [a-zA-Z]+)*,[0-9]+(.[0-9]+)?°[NESW]{1} [0-9]+(.[0-9]+)?°[NESW]{1}$";
	strcpy(loc, location);

	if(regexMatch(location, regexA)) {
		locationName = strtok(loc, ",");
		c1 = strtok(NULL, ",");
		c2 = strtok(NULL, ",");
		regexMatches = 1;
	}
	else if(regexMatch(location, regexB)) {
		locationName = strtok(loc, ",");
		c1 = strtok(NULL, " ");
		c2 = strtok(NULL, " ");
		regexMatches = 1;
	}
	else {
		return 0;
	}
	
	for(int i = 0; i<strlen(locationName); i++) {
		locationName[i] = tolower(locationName[i]);
	}

	*res = (GeoCoord *)palloc(sizeof(GeoCoord) + strlen(locationName) + 1);
	SET_VARSIZE(*res, sizeof(GeoCoord) + strlen(locationName) + 1);

	snprintf((*res)->Location, strlen(locationName) + 1, "%s", locationName);
	
	validCoords = initCoords(c1, c2, *res);
	if(regexMatches && validCoords) {
		return 1;
	}
	return 0;

}


/*****************************************************************************
 * Input/Output functions
 *****************************************************************************/

PG_FUNCTION_INFO_V1(gcoord_in);

Datum
gcoord_in(PG_FUNCTION_ARGS)
{
	char	   *str = PG_GETARG_CSTRING(0);
	GeoCoord   *result;
	int 		success;

	success = initLocation(str, &result);

	if (!success)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
				 errmsg("invalid input syntax for type %s: \"%s\"",
						"GeoCoord", str)));
	
	PG_RETURN_POINTER(result);
}

PG_FUNCTION_INFO_V1(gcoord_out);

Datum
gcoord_out(PG_FUNCTION_ARGS)
{
	GeoCoord   *geoCoord = (GeoCoord *) PG_GETARG_POINTER(0);
	char	   *result;

	result = psprintf("%s,%.7g°%c,%.7g°%c", 
							formatedLocationName(geoCoord->Location), 
							geoCoord->Latitude.degrees, 
							geoCoord->Latitude.direction, 
							geoCoord->Longitude.degrees, 
							geoCoord->Longitude.direction);

	PG_RETURN_CSTRING(result);
}


/*****************************************************************************
 * Operator functions
 *****************************************************************************/

static int
gcoord_cmp_internal(GeoCoord * a, GeoCoord * b)
{
	
	if(a->Latitude.degrees < b->Latitude.degrees) {
		return 1;
		
	}
	else if (a->Latitude.degrees > b->Latitude.degrees) {
		return -1;
		
	}
	else {
		if(a->Latitude.direction == 'N' && b->Latitude.direction != 'N') {
			return 1;
		}
		else if(b->Latitude.direction == 'N' && a->Latitude.direction != 'N') {
			return -1;
		}
		else {
			int medianDiffA;
			int medianDiffB;

			if(a->Longitude.degrees < fabs(a->Longitude.degrees - 180)) {
				medianDiffA = a->Longitude.degrees;
			}
			else {
				medianDiffA = fabs(a->Longitude.degrees -180);
			}

			if(b->Longitude.degrees < fabs(b->Longitude.degrees - 180)) {
				medianDiffB = b->Longitude.degrees;
			}
			else {
				medianDiffB = fabs(b->Longitude.degrees -180);
			}

			if(medianDiffA < medianDiffB) {
				return 1;
			}
			else if(medianDiffA > medianDiffB) {
				return -1;
			}
			else {

				if(a->Longitude.direction == 'E' && b->Longitude.direction != 'E') {
					return 1;
				}
				else if (b->Longitude.direction == 'E' && a->Longitude.direction != 'E') {
					return -1;
				}
				else {
					if(strcmp(a->Location, b->Location) > 0) {
						return 1;
					}
					else if(strcmp(a->Location, b->Location) < 0) {
						return -1;
					}
					else {
						return 0;
					}
				}
			}
		}
	}
}

static int
gcoord_cmp_internal_timezone(GeoCoord * a, GeoCoord * b)
{
	return ( (floor(a->Longitude.degrees/15) == floor(b->Longitude.degrees/15)) && 
			 (a->Longitude.direction == b->Longitude.direction)
		);
}


PG_FUNCTION_INFO_V1(gcoord_eq);

Datum
gcoord_eq(PG_FUNCTION_ARGS)
{
	GeoCoord   *a = (GeoCoord *) PG_GETARG_POINTER(0);
	GeoCoord   *b = (GeoCoord *) PG_GETARG_POINTER(1);

	PG_RETURN_BOOL(gcoord_cmp_internal(a,b) == 0);

}


PG_FUNCTION_INFO_V1(gcoord_gt);

Datum
gcoord_gt(PG_FUNCTION_ARGS)
{
	GeoCoord   *a = (GeoCoord *) PG_GETARG_POINTER(0);
	GeoCoord   *b = (GeoCoord *) PG_GETARG_POINTER(1);

	PG_RETURN_BOOL(gcoord_cmp_internal(a,b) > 0);

}


PG_FUNCTION_INFO_V1(gcoord_lt);

Datum
gcoord_lt(PG_FUNCTION_ARGS)
{
	GeoCoord   *a = (GeoCoord *) PG_GETARG_POINTER(0);
	GeoCoord   *b = (GeoCoord *) PG_GETARG_POINTER(1);

	PG_RETURN_BOOL(gcoord_cmp_internal(a,b) < 0);

}


PG_FUNCTION_INFO_V1(gcoord_neq);

Datum
gcoord_neq(PG_FUNCTION_ARGS)
{
	GeoCoord   *a = (GeoCoord *) PG_GETARG_POINTER(0);
	GeoCoord   *b = (GeoCoord *) PG_GETARG_POINTER(1);

	PG_RETURN_BOOL(gcoord_cmp_internal(a,b) != 0);

}

PG_FUNCTION_INFO_V1(gcoord_gt_or_eq);

Datum
gcoord_gt_or_eq(PG_FUNCTION_ARGS)
{
	GeoCoord   *a = (GeoCoord *) PG_GETARG_POINTER(0);
	GeoCoord   *b = (GeoCoord *) PG_GETARG_POINTER(1);

	PG_RETURN_BOOL(gcoord_cmp_internal(a,b) >= 0);

}

PG_FUNCTION_INFO_V1(gcoord_lt_or_eq);

Datum
gcoord_lt_or_eq(PG_FUNCTION_ARGS)
{
	GeoCoord   *a = (GeoCoord *) PG_GETARG_POINTER(0);
	GeoCoord   *b = (GeoCoord *) PG_GETARG_POINTER(1);

	PG_RETURN_BOOL(gcoord_cmp_internal(a,b) <= 0);

}

PG_FUNCTION_INFO_V1(gcoord_cmp);

Datum
gcoord_cmp(PG_FUNCTION_ARGS)
{
	GeoCoord    *a = (GeoCoord *) PG_GETARG_POINTER(0);
	GeoCoord    *b = (GeoCoord *) PG_GETARG_POINTER(1);

	PG_RETURN_INT32(gcoord_cmp_internal(a, b));
}


PG_FUNCTION_INFO_V1(gcoord_timezone_eq);

Datum
gcoord_timezone_eq(PG_FUNCTION_ARGS)
{
	GeoCoord   *a = (GeoCoord *) PG_GETARG_POINTER(0);
	GeoCoord   *b = (GeoCoord *) PG_GETARG_POINTER(1);

	PG_RETURN_BOOL(gcoord_cmp_internal_timezone(a,b) == 1);

}

PG_FUNCTION_INFO_V1(gcoord_timezone_neq);

Datum
gcoord_timezone_neq(PG_FUNCTION_ARGS)
{
	GeoCoord   *a = (GeoCoord *) PG_GETARG_POINTER(0);
	GeoCoord   *b = (GeoCoord *) PG_GETARG_POINTER(1);

	PG_RETURN_BOOL(gcoord_cmp_internal_timezone(a,b) == 0);

}


/*****************************************************************************
 * Additional function
 *****************************************************************************/

PG_FUNCTION_INFO_V1(convert2dms);

Datum
convert2dms(PG_FUNCTION_ARGS)
{
	GeoCoord   *geoCoord = (GeoCoord *) PG_GETARG_POINTER(0);

	int 	latDegrees, latMinute, latSecond, lonDegrees, lonMinute, lonSecond;
	char 	*result, *lat, *lon;

	latDegrees = floorf(geoCoord->Latitude.degrees);
	latMinute  = floorf(60 * fabs(geoCoord->Latitude.degrees - latDegrees));
	latSecond  = floorf(3600 * fabs(geoCoord->Latitude.degrees - latDegrees) - 60*latMinute);

	lonDegrees = floorf(geoCoord->Longitude.degrees);
	lonMinute  = floorf(60 * fabs(geoCoord->Longitude.degrees - lonDegrees));
	lonSecond  = floorf(3600 * fabs(geoCoord->Longitude.degrees - lonDegrees) - 60*lonMinute);

	lat = psprintf("%d°", latDegrees);
	if(latMinute > 0) {
		lat = psprintf("%s%d'", lat, latMinute);
	}
	if(latSecond > 0) {
		lat = psprintf("%s%d\"", lat, latSecond);
	}
	lat = psprintf("%s%c", lat, geoCoord->Latitude.direction);
	
	lon = psprintf("%d°", lonDegrees);
	if(lonMinute > 0) {
		lon = psprintf("%s%d'", lon, lonMinute);
	}
	if(lonSecond > 0) {
		lon = psprintf("%s%d\"", lon, lonSecond);
	}
	lon = psprintf("%s%c", lon, geoCoord->Longitude.direction);
	
	result = psprintf("%s,%s,%s", formatedLocationName(geoCoord->Location), lat, lon);
	
	PG_RETURN_CSTRING(result);

}


/*****************************************************************************
 * Hash Function
 *****************************************************************************/

PG_FUNCTION_INFO_V1(gcoord_hval);

Datum gcoord_hval(PG_FUNCTION_ARGS)
{
	GeoCoord   *geoCoord = (GeoCoord *) PG_GETARG_POINTER(0);
	char 	   *result;
	int 		h_code;

	result = psprintf("%s,%.7g°%c,%.7g°%c", 
						geoCoord->Location, 
						geoCoord->Latitude.degrees, 
						geoCoord->Latitude.direction, 
						geoCoord->Longitude.degrees, 
						geoCoord->Longitude.direction);

	h_code = DatumGetUInt32(hash_any((unsigned char *) result, strlen(result)));
	PG_RETURN_INT32(h_code);
}