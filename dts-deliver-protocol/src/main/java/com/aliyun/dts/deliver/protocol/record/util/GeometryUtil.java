package com.aliyun.dts.deliver.protocol.record.util;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.CoordinateSequence;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryCollection;
import com.vividsolutions.jts.geom.LineString;
import com.vividsolutions.jts.geom.MultiLineString;
import com.vividsolutions.jts.geom.MultiPoint;
import com.vividsolutions.jts.geom.MultiPolygon;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.geom.Polygon;
import com.vividsolutions.jts.io.ParseException;
import com.vividsolutions.jts.io.WKBReader;
import com.vividsolutions.jts.io.WKBWriter;
import com.vividsolutions.jts.io.WKTReader;

import java.io.IOException;
import java.io.StringWriter;
import java.io.Writer;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class GeometryUtil {

    private static final double SCALE = Math.pow(10.0D, 4.0);

    public static Geometry getGeometryFromBytes(byte[] geometryAsBytes) throws Exception {

        Geometry dbGeometry;
        // throw exception if length < 5
        if (geometryAsBytes.length < 5) {
            throw new Exception("Invalid geometry inputStream - less than five bytes");
        }

        //first four bytes of the geometry are the SRID,
        //followed by the actual WKB.  Determine the SRID
        //first 4 bytes for srid
        byte[] sridBytes = new byte[4];
        System.arraycopy(geometryAsBytes, 0, sridBytes, 0, 4);
        boolean bigEndian = (geometryAsBytes[4] == 0x00);
        // parse srid
        int srid = 0;
        if (bigEndian) {
            for (int i = 0; i < sridBytes.length; i++) {
                srid = (srid << 8) + (sridBytes[i] & 0xff);
            }
        } else {
            for (int i = 0; i < sridBytes.length; i++) {
                srid += (sridBytes[i] & 0xff) << (8 * i);
            }
        }

        //use the JTS WKBReader for WKB parsing
        WKBReader wkbReader = new WKBReader();
        //copy the byte array, removing the first four
        //SRID bytes
        byte[] wkb = new byte[geometryAsBytes.length - 4];
        System.arraycopy(geometryAsBytes, 4, wkb, 0, wkb.length);
        dbGeometry = wkbReader.read(wkb);
        dbGeometry.setSRID(srid);

        return dbGeometry;
    }

    public static String fromWKBToWKTText(ByteBuffer data) throws ParseException {
        if (null == data) {
            return null;
        } else {
            WKBReader reader = new WKBReader();
            Geometry geometry = reader.read(data.array());
            return geometry.toText();
        }
    }

    public static byte[] fromWKTToWKB(String wkt) throws ParseException {
        if (wkt == null) {
            return null;
        }
        WKBWriter writer = new WKBWriter();
        WKTReader reader = new WKTReader();
        return writer.write(reader.read(wkt));
    }

    public static String formatGeometry(String wkt) throws Exception {
        WKTReader reader = new WKTReader();
        Geometry geometry = reader.read(wkt);

        return formatGeometry(geometry);
    }

    public static String formatGeometry(Geometry geometry) throws Exception {

        if (geometry == null) {
            return null;
        }

        List<Object> objs = create(geometry);
        if (objs == null || objs.size() <= 0) {
            return null;
        }

        StringWriter writer = new StringWriter();
        serializeGeometry(objs, writer);

        return writer.toString();
    }

    private static void serializeGeometry(List<?> objs, StringWriter out) throws IOException {
        if (objs == null || objs.size() <= 0) {
            return;
        }

        int size = objs.size();

        if (size > 1) {
            out.write("(");
        }

        for (int i = 0; i < size; i++) {
            Object obj = objs.get(i);

            if (obj instanceof CoordinateSequenceEncoder) {
                ((CoordinateSequenceEncoder) obj).write2String(out);
            } else if (obj instanceof List<?>) {
                serializeGeometry((List<?>) obj, out);
            } else {
                throw new IllegalArgumentException("Unable to serialize object " + obj);
            }

            if (i < size - 1) {
                out.write(",");
            }
        }

        if (size > 1) {
            out.write(")");
        }
    }

    private static List<Object> create(Geometry geometry) {

        if ((geometry instanceof Point)) {

            return createPoint((Point) geometry);
        }

        if ((geometry instanceof LineString)) {
            return createLine((LineString) geometry);
        }

        if ((geometry instanceof Polygon)) {
            return createPolygon((Polygon) geometry);
        }

        if ((geometry instanceof MultiPoint)) {
            return createMultiPoint((MultiPoint) geometry);
        }

        if ((geometry instanceof MultiLineString)) {
            return createMultiLine((MultiLineString) geometry);
        }

        if ((geometry instanceof MultiPolygon)) {
            return createMultiPolygon((MultiPolygon) geometry);
        }

        if ((geometry instanceof GeometryCollection)) {
            return createGeometryCollection((GeometryCollection) geometry);
        }

        throw new IllegalArgumentException("Unable to serialize object " + geometry);
    }

    private static List<Object> createPoint(Point point) {

        List<Object> obj = new ArrayList<Object>();

        obj.add(new CoordinateSequenceEncoder(point.getCoordinateSequence(), SCALE));

        return obj;
    }

    private static List<Object> createLine(LineString line) {

        List<Object> obj = new ArrayList<Object>();

        obj.add(new CoordinateSequenceEncoder(line.getCoordinateSequence(), SCALE));

        return obj;
    }

    private static List<Object> createMultiPoint(MultiPoint mpoint) {
        List<Object> obj = new ArrayList<Object>();
        obj.add(toList(mpoint));

        return obj;
    }

    private static List<Object> createMultiLine(MultiLineString mline) {
        List<Object> obj = new ArrayList<Object>();
        obj.add(toList(mline));

        return obj;
    }

    private static List<Object> createPolygon(Polygon poly) {
        List<Object> obj = new ArrayList<Object>();
        obj.add(toList(poly));

        return obj;
    }

    private static List<Object> createMultiPolygon(MultiPolygon mpoly) {
        List<Object> obj = new ArrayList<Object>();

        obj.add(toList(mpoly));

        return obj;
    }

    private static List<Object> createGeometryCollection(GeometryCollection gcol) {
        List<Object> obj = new ArrayList<Object>();

        ArrayList<Object> geoms = new ArrayList<Object>(gcol.getNumGeometries());
        for (int i = 0; i < gcol.getNumGeometries(); i++) {
            geoms.add(create(gcol.getGeometryN(i)));
        }

        obj.add(geoms);

        return obj;
    }

    private static List<Object> toList(Polygon poly) {
        ArrayList<Object> list = new ArrayList<Object>();

        list.add(new CoordinateSequenceEncoder(poly.getExteriorRing().getCoordinateSequence(), SCALE));

        for (int i = 0; i < poly.getNumInteriorRing(); i++) {
            list.add(new CoordinateSequenceEncoder(poly
                    .getInteriorRingN(i)
                    .getCoordinateSequence(), SCALE));
        }

        return list;
    }

    private static List<Object> toList(GeometryCollection mgeom) {
        ArrayList<Object> list = new ArrayList<Object>(mgeom.getNumGeometries());

        for (int i = 0; i < mgeom.getNumGeometries(); i++) {
            Geometry g = mgeom.getGeometryN(i);
            if ((g instanceof Polygon)) {
                list.add(toList((Polygon) g));
            } else if ((g instanceof LineString)) {
                list.add(new CoordinateSequenceEncoder(((LineString) g)
                        .getCoordinateSequence(), SCALE));
            } else if ((g instanceof Point)) {
                list.add(new CoordinateSequenceEncoder(((Point) g).getCoordinateSequence(), SCALE));
            }
        }

        return list;
    }

    static class CoordinateSequenceEncoder {
        CoordinateSequence seq;
        double scale;

        CoordinateSequenceEncoder(CoordinateSequence seq, double scale) {
            this.seq = seq;
            this.scale = scale;
        }

        public void write2String(Writer out) throws IOException {
            int size = this.seq.size();

            if (size > 1) {
                out.write("(");
            }

            for (int i = 0; i < this.seq.size(); i++) {
                Coordinate coord = this.seq.getCoordinate(i);
                out.write("(");
                out.write(String.valueOf(coord.x));
                out.write(",");
                out.write(String.valueOf(coord.y));

                if (!Double.isNaN(coord.z)) {
                    out.write(",");
                    out.write(String.valueOf(coord.z));
                }

                out.write(")");

                if (i < this.seq.size() - 1) {
                    out.write(",");
                }
            }

            if (size > 1) {
                out.write(")");
            }
        }
    }
}
