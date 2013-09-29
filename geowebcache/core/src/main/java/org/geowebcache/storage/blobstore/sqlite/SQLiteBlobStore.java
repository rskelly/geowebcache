/**
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU Lesser General Public License
 *  along with this program.  If not, see <http://www.gnu.org/licenses/>.
 * 
 * @author Arne Kepp / The Open Planning Project 2009 
 *
 */
package org.geowebcache.storage.blobstore.sqlite;

import static org.geowebcache.storage.blobstore.file.FilePathUtils.*;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.geowebcache.config.ConfigurationException;
import org.geowebcache.io.ByteArrayResource;
import org.geowebcache.io.Resource;
import org.geowebcache.storage.BlobStore;
import org.geowebcache.storage.BlobStoreListener;
import org.geowebcache.storage.BlobStoreListenerList;
import org.geowebcache.storage.DefaultStorageFinder;
import org.geowebcache.storage.StorageException;
import org.geowebcache.storage.TileObject;
import org.geowebcache.storage.TileRange;

/**
 * See BlobStore interface description for details
 * 
 */
public class SQLiteBlobStore implements BlobStore {

	private static Log log = LogFactory
			.getLog(org.geowebcache.storage.blobstore.sqlite.SQLiteBlobStore.class);

	public static final int BUFFER_SIZE = 32768;

	private final String path;

	private final BlobStoreListenerList listeners = new BlobStoreListenerList();

	private final Map<String, Connection> connections = Collections
			.synchronizedMap(new HashMap<String, Connection>());

	private static final Object createLock = new Object();

	/**
	 * Create a new SQLiteBlobStore which will store SQLite DB files in the
	 * given directory.
	 * 
	 * @param defStoreFinder
	 * @throws StorageException
	 * @throws ConfigurationException
	 */
	public SQLiteBlobStore(DefaultStorageFinder defStoreFinder)
			throws StorageException, ConfigurationException {
		this(defStoreFinder.getDefaultPath());
		try {
			Class.forName("org.sqlite.JDBC");
		} catch (ClassNotFoundException e) {
			throw new StorageException("Couldn't load SQLite driver class.");
		}
	}

	/**
	 * Create a new SQLiteBlobStore which will store SQLite DB files in the
	 * given directory.
	 * 
	 * @param path
	 * @throws StorageException
	 */
	public SQLiteBlobStore(String path) throws StorageException {
		this.path = path;
		log.info("Configured SQLiteBlobStore with path " + path);
	}

	/**
	 * Gets a {@link Connection} for the layer's db file.
	 * 
	 * @param layerName
	 * @return
	 * @throws StorageException
	 */
	@SuppressWarnings("resource")
	private Connection getConnection(String layerName) throws StorageException {
		try {
			Connection conn = null;
			if ((conn = connections.get(layerName)) != null && !conn.isClosed())
				return conn;

			File file = getLayerPath(layerName);
			synchronized (createLock) {
				if (!file.exists()) {
					conn = createDatabase(file);
				} else {
					conn = DriverManager.getConnection("jdbc:sqlite:"
							+ file.getAbsolutePath());				
				}
				connections.put(layerName, conn);
			}
			return conn;
		} catch (SQLException e) {
			throw new StorageException(e.getMessage());
		}
	}

	/**
	 * Create the new db file and tables.
	 * 
	 * @param file
	 * @throws SQLException
	 */
	private Connection createDatabase(File file) throws SQLException {
		log.info("Creating SQLite tile database " + file.getName());
		Connection conn = DriverManager.getConnection("jdbc:sqlite:"
				+ file.getAbsolutePath());
		conn.createStatement()
				.execute(
						"CREATE TABLE tiles (grid_set_id text, x integer, y integer, z integer, type text, data blob)");
		conn.createStatement().execute(
				"CREATE TABLE meta (key text, value text");
		return conn;
	}

	/**
	 * Destroy method for Spring
	 */
	public void destroy() {
		log.info("Closing SQLiteBlobStore connections.");
		for (Connection conn : connections.values()) {
			try {
				conn.close();
			} catch (Exception e) {
			}
		}
	}

	/**
	 * @see org.geowebcache.storage.BlobStore#delete(java.lang.String)
	 */
	public boolean delete(final String layerName) throws StorageException {
		log.info("Deleting SQLite cached layer " + layerName);
		return getLayerPath(layerName).delete();
	}

	/**
	 * @throws StorageException
	 * @see org.geowebcache.storage.BlobStore#deleteByGridsetId(java.lang.String,
	 *      java.lang.String)
	 */
	public boolean deleteByGridsetId(final String layerName,
			final String gridSetId) throws StorageException {
		log.info("Deleting SQLite cached layer " + layerName + "; " + gridSetId);
		Connection conn = getConnection(layerName);
		try {
			PreparedStatement stmt = conn
					.prepareStatement("DELETE FROM tiles WHERE grid_set_id=?");
			try {
				stmt.setString(1, gridSetId);
				return true;
			} finally {
				stmt.close();
			}
		} catch (SQLException e) {
			throw new StorageException(e.getMessage());
		}
	}

	/**
	 * Renames the layer directory for layer {@code oldLayerName} to
	 * {@code newLayerName}
	 * 
	 * @return true if the directory for the layer was renamed, or the original
	 *         directory didn't exist in first place. {@code false} if the
	 *         original directory exists but can't be renamed to the target
	 *         directory
	 * @throws StorageException
	 *             if the target directory already exists
	 * @see org.geowebcache.storage.BlobStore#rename
	 */
	public boolean rename(final String oldLayerName, final String newLayerName)
			throws StorageException {
		log.info("Renaming SQLite cached layer " + oldLayerName + " to " + newLayerName);
		return getLayerPath(oldLayerName).renameTo(getLayerPath(newLayerName));
	}

	/**
	 * Returns a {@link File} object pointing to the SQLite database file for
	 * this layer.
	 * 
	 * @param layerName
	 * @return
	 */
	private File getLayerPath(String layerName) {
		return new File(path, filteredLayerName(layerName) + ".sqlite");
	}

	/**
	 * Delete a particular tile
	 */
	public boolean delete(TileObject stObj) throws StorageException {
		final String layerName = stObj.getLayerName();
		final String gridSetId = stObj.getGridSetId();
		final String format = stObj.getBlobFormat();
		final long[] xyz = stObj.getXYZ();
		log.info("Deleting SQLite cached layer " + layerName + " (" + format + ")");
		Connection conn = getConnection(layerName);
		try {
			PreparedStatement stmt = conn
					.prepareStatement("DELETE FROM tiles WHERE grid_set_id=? AND x=? AND y=? AND z=? AND type=?");
			try {
				stmt.setString(1, gridSetId);
				stmt.setLong(2, xyz[0]);
				stmt.setLong(3, xyz[1]);
				stmt.setLong(4, xyz[2]);
				stmt.setString(5, format);
				stmt.execute();
				listeners.sendTileDeleted(stObj);
				return true;
			} finally {
				stmt.close();
			}
		} catch (SQLException e) {
			throw new StorageException(e.getMessage());
		}
	}

	/**
	 * Delete tiles within a range.
	 */
	public boolean delete(TileRange trObj) throws StorageException {
		throw new StorageException("Not implemented yet!");
		/*
		 * int count = 0; final String layerName = trObj.getLayerName(); final
		 * String gridSetId = trObj.getGridSetId(); final String blobFormat =
		 * trObj.getMimeType().getFormat(); final String parametersId =
		 * trObj.getParametersId();
		 * 
		 * File[] srsZoomDirs = layerPath.listFiles(tileFinder);
		 * 
		 * final String gridsetPrefix = filteredGridSetId(gridSetId); for (File
		 * srsZoomParamId : srsZoomDirs) { int zoomLevel =
		 * findZoomLevel(gridsetPrefix, srsZoomParamId.getName()); File[]
		 * intermediates = srsZoomParamId.listFiles(tileFinder);
		 * 
		 * for (File imd : intermediates) { File[] tiles =
		 * imd.listFiles(tileFinder); long length;
		 * 
		 * for (File tile : tiles) { length = tile.length(); boolean deleted =
		 * tile.delete(); if (deleted) { String[] coords =
		 * tile.getName().split("\\.")[0].split("_"); long x =
		 * Long.parseLong(coords[0]); long y = Long.parseLong(coords[1]);
		 * listeners.sendTileDeleted(layerName, gridSetId, blobFormat,
		 * parametersId, x, y, zoomLevel, length); count++; } }
		 * 
		 * // Try deleting the directory (will be done only if the directory is
		 * empty) if (imd.delete()) { //
		 * listeners.sendDirectoryDeleted(layerName); } }
		 * 
		 * // Try deleting the zoom directory (will be done only if the
		 * directory is empty) if (srsZoomParamId.delete()) { count++; //
		 * listeners.sendDirectoryDeleted(layerName); } }
		 * 
		 * log.info("Truncated " + count + " tiles");
		 */
	}

	/**
	 * Set the blob property of a TileObject.
	 * 
	 * @param stObj
	 *            the tile to load. Its setBlob() method will be called.
	 * @return true if successful, false otherwise
	 */
	public boolean get(TileObject stObj) throws StorageException {
		final String layerName = stObj.getLayerName();
		final String gridSetId = stObj.getGridSetId();
		final String format = stObj.getBlobFormat();
		final long[] xyz = stObj.getXYZ();
		Connection conn = getConnection(layerName);
		try {
			PreparedStatement stmt = conn
					.prepareStatement("SELECT data FROM tiles WHERE grid_set_id=? AND x=? AND y=? AND z=? AND type=?");
			try {
				stmt.setString(1, gridSetId);
				stmt.setLong(2, xyz[0]);
				stmt.setLong(3, xyz[1]);
				stmt.setLong(4, xyz[2]);
				stmt.setString(5, format);
				ResultSet rslt = stmt.executeQuery();
				if (rslt.next()) {
					Resource r = new ByteArrayResource(rslt.getBytes("data"));
					stObj.setBlob(r);
					stObj.setCreated(r.getLastModified());
					stObj.setBlobSize((int) r.getSize());
					return true;
				}
				return false;
			} finally {
				stmt.close();
			}
		} catch (SQLException e) {
			throw new StorageException(e.getMessage());
		}
	}

	/**
	 * Store a tile.
	 */
	public void put(TileObject stObj) throws StorageException {
		final String layerName = stObj.getLayerName();
		final String gridSetId = stObj.getGridSetId();
		final String format = stObj.getBlobFormat();
		final long[] xyz = stObj.getXYZ();
		Connection conn = getConnection(layerName);
		try {
			PreparedStatement stmt = conn
					.prepareStatement("INSERT OR REPLACE INTO tiles (grid_set_id, x, y, z, type) VALUES (?, ?, ?, ?, type)");
			try {
				stmt.setString(1, gridSetId);
				stmt.setLong(2, xyz[0]);
				stmt.setLong(3, xyz[1]);
				stmt.setLong(4, xyz[2]);
				stmt.setString(5, format);
				stmt.execute();
				// TODO: Code for update.
				// if (existed) {
				// listeners.sendTileUpdated(stObj, oldSize);
				// } else {
				listeners.sendTileStored(stObj);
				// }
			} finally {
				stmt.close();
			}
		} catch (SQLException e) {
			throw new StorageException(e.getMessage());
		}
	}

	public void clear() throws StorageException {
		throw new StorageException("Not implemented yet!");
	}

	/**
	 * Add an event listener
	 */
	public void addListener(BlobStoreListener listener) {
		listeners.addListener(listener);
	}

	/**
	 * Remove an event listener
	 */
	public boolean removeListener(BlobStoreListener listener) {
		return listeners.removeListener(listener);
	}

	/**
	 * @see org.geowebcache.storage.BlobStore#getLayerMetadata(java.lang.String,
	 *      java.lang.String)
	 */
	public String getLayerMetadata(final String layerName, final String key) {
		Connection conn = null;
		try {
			conn = getConnection(layerName);
			PreparedStatement stmt = conn
					.prepareStatement("SELECT value FROM meta WHERE key=?");
			try {
				stmt.setString(1, key);
				ResultSet rslt = stmt.executeQuery();
				if (rslt.next())
					return rslt.getString("value");
			} finally {
				stmt.close();
			}
		} catch (Exception e) {
			// do nothing.
		}
		return null;
	}

	/**
	 * @see org.geowebcache.storage.BlobStore#putLayerMetadata(java.lang.String,
	 *      java.lang.String, java.lang.String)
	 */
	public void putLayerMetadata(final String layerName, final String key,
			final String value) {
		Connection conn = null;
		try {
			conn = getConnection(layerName);
			PreparedStatement stmt = conn
					.prepareStatement("INSERT OR REPLACE INTO meta (key, value) VALUES (?, ?)");
			try {
				stmt.setString(1, key);
				stmt.setString(2, value);
				stmt.execute();
			} finally {
				stmt.close();
			}
		} catch (Exception e) {
			// do nothing.
		}
	}

	/**
	 * Returns a {@link Properties} object containing the metadata for the given
	 * layer.
	 * 
	 * @param layerName
	 * @return
	 */
	public Properties getLayerMetadata(final String layerName) {
		Properties props = new Properties();
		Connection conn = null;
		try {
			conn = getConnection(layerName);
			PreparedStatement stmt = conn
					.prepareStatement("SELECT key, value FROM meta");
			try {
				ResultSet rslt = stmt.executeQuery();
				while (rslt.next())
					props.put(rslt.getString("key"), rslt.getString("value"));
			} finally {
				stmt.close();
			}
		} catch (Exception e) {
			// do nothing.
		}
		return props;
	}

}
