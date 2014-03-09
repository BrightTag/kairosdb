package org.kairosdb.core.datapoints;

import com.google.gson.JsonElement;
import org.kairosdb.core.DataPoint;

import java.io.DataInput;
import java.io.IOException;

import static org.kairosdb.core.DataPoint.GROUP_NUMBER;

/**
 * @author codyaray
 * @since 3/9/2013
 */
public class OpaqueStormDataPointFactory implements DataPointFactory
{
	public static final String DST_STORM_OPAQUE = "kairos_storm_opaque";

	@Override
	public String getDataStoreType()
	{
		return DST_STORM_OPAQUE;
	}

	@Override
	public String getGroupType()
	{
		return GROUP_NUMBER;
	}

	@Override
	public DataPoint getDataPoint(long timestamp, JsonElement json) throws IOException
	{
		OpaqueStormDataPoint ret = new OpaqueStormDataPoint(timestamp, json.getAsString());
		return ret;
	}

	@Override
	public DataPoint getDataPoint(long timestamp, DataInput buffer) throws IOException
	{
		OpaqueStormDataPoint ret = new OpaqueStormDataPoint(timestamp, buffer.readUTF());
		return ret;
	}
}
