package org.kairosdb.core.datapoints;

import org.json.JSONException;
import org.json.JSONWriter;

import java.io.DataOutput;
import java.io.IOException;

/**
 * @author codyaray
 * @since 3/9/2013
 */
public class OpaqueStormDataPoint extends DataPointHelper
{
	public static final String API_TYPE = "storm_opaque";

	private final String m_value;

	public OpaqueStormDataPoint(long timestamp, String value)
	{
		super(timestamp);
		m_value = value;
	}

	@Override
	public void writeValueToBuffer(DataOutput buffer) throws IOException
	{
		buffer.writeUTF(m_value);
	}

	@Override
	public void writeValueToJson(JSONWriter writer) throws JSONException
	{
		writer.value(m_value);
	}

	@Override
	public String getApiDataType()
	{
		return API_TYPE;
	}

	@Override
	public String getDataStoreDataType()
	{
		return OpaqueStormDataPointFactory.DST_STORM_OPAQUE;
	}

	@Override
	public boolean isLong()
	{
		return true;
	}

	@Override
	public long getLongValue()
	{
		return getCurrentValue();
	}

	@Override
	public boolean isDouble()
	{
		return true;
	}

	@Override
	public double getDoubleValue()
	{
		return (double)getCurrentValue();
	}

  private long getCurrentValue() {
    // Let's not actually use a JSON parser here. Smaller footprint. But is it more efficient???
    if (m_value.charAt(0) != '[' || m_value.charAt(m_value.length()-1) != ']') {
      return 0;
    }
    String[] parts = m_value.substring(1, m_value.length()-1).split(",");
    if (parts.length != 3) {
      return 0;
    }
    try {
      return Long.parseLong(parts[1]);
    } catch (NumberFormatException e) {
      return 0;
    }
  }
}
