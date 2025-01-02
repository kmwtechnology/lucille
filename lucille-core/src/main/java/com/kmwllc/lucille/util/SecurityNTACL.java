package com.kmwllc.lucille.util;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

public class SecurityNTACL {

	/**
	 * Definitions for "standard" windows perms
	 */

	// These are well-known Security Identifiers (SIDs) used to represent common
	// user and group accounts in Windows,
	public static final Map<String, String> SEC_ACE_SIDS = Map.ofEntries(Map.entry("S-1-0-0", "NULL"),
			Map.entry("S-1-1-0", "EVERYONE"), Map.entry("S-1-2-0", "LOCAL"), Map.entry("S-1-2-1", "CONSOLE_LOGON"),
			Map.entry("S-1-3-0", "CREATOR_OWNER"), Map.entry("S-1-3-1", "CREATOR_GROUP"),
			Map.entry("S-1-3-2", "OWNER_SERVER"), Map.entry("S-1-3-3", "GROUP_SERVER"),
			Map.entry("S-1-3-4", "OWNER_RIGHTS"), Map.entry("S-1-5", "NT_AUTHORITY"), Map.entry("S-1-5-1", "DIALUP"),
			Map.entry("S-1-5-2", "NETWORK"), Map.entry("S-1-5-3", "BATCH"), Map.entry("S-1-5-4", "INTERACTIVE"),
			Map.entry("S-1-5-5-x-y", "LOGON_ID"), Map.entry("S-1-5-6", "SERVICE"), Map.entry("S-1-5-7", "ANONYMOUS"),
			Map.entry("S-1-5-8", "PROXY"), Map.entry("S-1-5-9", "ENTERPRISE_DOMAIN_CONTROLLERS"),
			Map.entry("S-1-5-10", "PRINCIPAL_SELF"), Map.entry("S-1-5-11", "AUTHENTICATED_USERS"),
			Map.entry("S-1-5-12", "RESTRICTED_CODE"), Map.entry("S-1-5-13", "TERMINAL_SERVER_USER"),
			Map.entry("S-1-5-14", "REMOTE_INTERACTIVE_LOGON"), Map.entry("S-1-5-15", "THIS_ORGANIZATION"),
			Map.entry("S-1-5-17", "IUSR"), Map.entry("S-1-5-18", "LOCAL_SYSTEM"),
			Map.entry("S-1-5-19", "LOCAL_SERVICE"), Map.entry("S-1-5-20", "NETWORK_SERVICE"),
			Map.entry("S-1-5-21-[-0-9]+-498", "ENTERPRISE_READONLY_DOMAIN_CONTROLLERS"),
			Map.entry("S-1-5-21-0-0-0-496", "COMPOUNDED_AUTHENTICATION"),
			Map.entry("S-1-5-21-0-0-0-497", "CLAIMS_VALID"), Map.entry("S-1-5-21-[-0-9]+-500", "ADMINISTRATOR"),
			Map.entry("S-1-5-21-[-0-9]+-501", "GUEST"), Map.entry("S-1-5-21-[-0-9]+-502", "KRBTGT"),
			Map.entry("S-1-5-21-[-0-9]+-512", "DOMAIN_ADMINS"), Map.entry("S-1-5-21-[-0-9]+-513", "DOMAIN_USERS"),
			Map.entry("S-1-5-21-[-0-9]+-514", "DOMAIN_GUESTS"), Map.entry("S-1-5-21-[-0-9]+-515", "DOMAIN_COMPUTERS"),
			Map.entry("S-1-5-21-[-0-9]+-516", "DOMAIN_DOMAIN_CONTROLLERS"),
			Map.entry("S-1-5-21-[-0-9]+-517", "CERT_PUBLISHERS"),
			Map.entry("S-1-5-21-[-0-9]+-518", "SCHEMA_ADMINISTRATORS"),
			Map.entry("S-1-5-21-[-0-9]+-519", "ENTERPRISE_ADMINS"),
			Map.entry("S-1-5-21-[-0-9]+-520", "GROUP_POLICY_CREATOR_OWNERS"),
			Map.entry("S-1-5-21-[-0-9]+-521", "READONLY_DOMAIN_CONTROLLERS"),
			Map.entry("S-1-5-21-[-0-9]+-522", "CLONEABLE_CONTROLLERS"),
			Map.entry("S-1-5-21-[-0-9]+-525", "PROTECTED_USERS"), Map.entry("S-1-5-21-[-0-9]+-526", "KEY_ADMINS"),
			Map.entry("S-1-5-21-[-0-9]+-527", "ENTERPRISE_KEY_ADMINS"),
			Map.entry("S-1-5-21-[-0-9]+-553", "RAS_SERVERS"),
			Map.entry("S-1-5-21-[-0-9]+-571", "ALLOWED_RODC_PASSWORD_REPLICATION_GROUP"),
			Map.entry("S-1-5-21-[-0-9]+-572", "DENIED_RODC_PASSWORD_REPLICATION_GROUP"),
			Map.entry("S-1-5-32", "BUILTIN"), Map.entry("S-1-5-32-544", "BUILTIN_ADMINISTRATORS"),
			Map.entry("S-1-5-32-545", "BUILTIN_USERS"), Map.entry("S-1-5-32-546", "BUILTIN_GUESTS"),
			Map.entry("S-1-5-32-547", "POWER_USERS"), Map.entry("S-1-5-32-548", "ACCOUNT_OPERATORS"),
			Map.entry("S-1-5-32-549", "SERVER_OPERATORS"), Map.entry("S-1-5-32-550", "PRINTER_OPERATORS"),
			Map.entry("S-1-5-32-551", "BACKUP_OPERATORS"), Map.entry("S-1-5-32-552", "REPLICATOR"),
			Map.entry("S-1-5-32-554", "ALIAS_PREW2KCOMPACC"), Map.entry("S-1-5-32-555", "REMOTE_DESKTOP"),
			Map.entry("S-1-5-32-556", "NETWORK_CONFIGURATION_OPS"),
			Map.entry("S-1-5-32-557", "INCOMING_FOREST_TRUST_BUILDERS"), Map.entry("S-1-5-32-558", "PERFMON_USERS"),
			Map.entry("S-1-5-32-559", "PERFLOG_USERS"), Map.entry("S-1-5-32-560", "WINDOWS_AUTHORIZATION_ACCESS_GROUP"),
			Map.entry("S-1-5-32-561", "TERMINAL_SERVER_LICENSE_SERVERS"),
			Map.entry("S-1-5-32-562", "DISTRIBUTED_COM_USERS"), Map.entry("S-1-5-32-568", "IIS_IUSRS"),
			Map.entry("S-1-5-32-569", "CRYPTOGRAPHIC_OPERATORS"), Map.entry("S-1-5-32-573", "EVENT_LOG_READERS"),
			Map.entry("S-1-5-32-574", "CERTIFICATE_SERVICE_DCOM_ACCESS"),
			Map.entry("S-1-5-32-575", "RDS_REMOTE_ACCESS_SERVERS"), Map.entry("S-1-5-32-576", "RDS_ENDPOINT_SERVERS"),
			Map.entry("S-1-5-32-577", "RDS_MANAGEMENT_SERVERS"), Map.entry("S-1-5-32-578", "HYPER_V_ADMINS"),
			Map.entry("S-1-5-32-579", "ACCESS_CONTROL_ASSISTANCE_OPS"),
			Map.entry("S-1-5-32-580", "REMOTE_MANAGEMENT_USERS"), Map.entry("S-1-5-33", "WRITE_RESTRICTED_CODE"),
			Map.entry("S-1-5-64-10", "NTLM_AUTHENTICATION"), Map.entry("S-1-5-64-14", "SCHANNEL_AUTHENTICATION"),
			Map.entry("S-1-5-64-21", "DIGEST_AUTHENTICATION"), Map.entry("S-1-5-65-1", "THIS_ORGANIZATION_CERTIFICATE"),
			Map.entry("S-1-5-80", "NT_SERVICE"), Map.entry("S-1-5-84-0-0-0-0-0", "USER_MODE_DRIVERS"),
			Map.entry("S-1-5-113", "LOCAL_ACCOUNT"),
			Map.entry("S-1-5-114", "LOCAL_ACCOUNT_AND_MEMBER_OF_ADMINISTRATORS_GROUP"),
			Map.entry("S-1-5-1000", "OTHER_ORGANIZATION"), Map.entry("S-1-15-2-1", "ALL_APP_PACKAGES"),
			Map.entry("S-1-16-0", "ML_UNTRUSTED"), Map.entry("S-1-16-4096", "ML_LOW"),
			Map.entry("S-1-16-8192", "ML_MEDIUM"), Map.entry("S-1-16-8448", "ML_MEDIUM_PLUS"),
			Map.entry("S-1-16-12288", "ML_HIGH"), Map.entry("S-1-16-16384", "ML_SYSTEM"),
			Map.entry("S-1-16-20480", "ML_PROTECTED_PROCESS"), Map.entry("S-1-16-28672", "ML_SECURE_PROCESS"),
			Map.entry("S-1-18-1", "AUTHENTICATION_AUTHORITY_ASSERTED_IDENTITY"),
			Map.entry("S-1-18-2", "SERVICE_ASSERTED_IDENTITY"), Map.entry("S-1-18-3", "FRESH_PUBLIC_KEY_IDENTITY"),
			Map.entry("S-1-18-4", "KEY_TRUST_IDENTITY"), Map.entry("S-1-18-5", "KEY_PROPERTY_MFA"),
			Map.entry("S-1-18-6", "KEY_PROPERTY_ATTESTATION"));

	// These are standard access rights defined by Windows for managing object-level
	// permissions.
	public static final Map<String, Integer> SEC_ACE_ACCESS = Map.ofEntries(
	        Map.entry("SYNCHRONIZE_ACCESS", 0x100000),
	        Map.entry("DELETE_ACCESS", 0x010000),
	        Map.entry("READ_CONTROL_ACCESS", 0x020000),
	        Map.entry("WRITE_OWNER_ACCESS", 0x080000),
	        Map.entry("WRITE_DAC_ACCESS", 0x040000)
	    );
	
	// SEC_ACE_PERMISSIONS: These are permission masks used in Windows to define
	// specific access levels for objects like files, directories, and registry keys
	public static final Map<String, Integer> SEC_ACE_PERMISSIONS = Map.ofEntries(Map.entry("FULL_CONTROL", 0x1f01ff),
			Map.entry("LIST_FOLDER_OR_READ_DATA", 0x100001), Map.entry("TRAVERSE_FOLDER_OR_EXECUTE_FILE", 0x100020),
			Map.entry("READ_ATTRIBUTES", 0x100080), Map.entry("READ_EXTENDED_ATTRIBUTES", 0x100008),
			Map.entry("CREATE_FILES_OR_WRITE_DATA", 0x100002), Map.entry("CREATE_FOLDERS_OR_APPEND_DATA", 0x100004),
			Map.entry("WRITE_ATTRIBUTES", 0x100100), Map.entry("WRITE_EXTENDED_ATTRIBUTES", 0x100010),
			Map.entry("DELETE_SUBFOLDERS_AND_FILES", 0x100040), Map.entry("DELETE", 0x110000),
			Map.entry("READ_PERMISSIONS", 0x120000), Map.entry("CHANGE_PERMISSIONS", 0x140000),
			Map.entry("TAKE_OWNERSHIP", 0x180000));

	static class Sid {
		private byte[] data;
		private int offset;

		public Sid(byte[] data, int offset) {
			this.data = data;
			this.offset = offset;
		}

		public String getName() {
			ByteBuffer buffer = ByteBuffer.wrap(data, offset, data.length - offset);
			int revision = buffer.get();
			int numAuths = buffer.get();

			if (numAuths > 15) {
				throw new IllegalArgumentException("numAuths > 16");
			}

			byte[] idAuth = new byte[6];
			buffer.get(idAuth);

			StringBuilder sidString = new StringBuilder("S-").append(revision).append("-")
					.append(idAuth[idAuth.length - 1] & 0xFF);

			for (int i = 0; i < numAuths; i++) {
				sidString.append("-").append(buffer.getInt());
			}

			for (Map.Entry<String, String> entry : SEC_ACE_SIDS.entrySet()) {
				if (Pattern.matches(entry.getKey(), sidString)) {
					return entry.getValue();
				}
			}

			return sidString.toString();
		}
	}

	static class SecurityAce {
		private int accessMask;
		private Sid trustee;

		public SecurityAce(int accessMask, Sid trustee) {
			this.accessMask = accessMask;
			this.trustee = trustee;
		}

		public List<String> getPermissions() {
			List<String> permissions = new ArrayList<>();

			for (Map.Entry<String, Integer> entry : SEC_ACE_PERMISSIONS.entrySet()) {
				if ((accessMask & entry.getValue()) == entry.getValue()) {
					permissions.add(entry.getKey());
				}
			}

			if (permissions.contains("FULL_CONTROL")) {
				return Collections.singletonList("FULL_CONTROL");
			}

			return permissions;
		}

		public List<String> getAccess() {
			List<String> accessList = new ArrayList<>();

			for (Map.Entry<String, Integer> entry : SEC_ACE_ACCESS.entrySet()) {
				if ((accessMask & entry.getValue()) == entry.getValue()) {
					accessList.add(entry.getKey());
				}
			}

			return accessList;
		}

		public Sid getTrustee() {
			return trustee;
		}
	}

	static class SecurityAcl {
		private byte[] data;
		private int offset;

		public SecurityAcl(byte[] data, int offset) {
			this.data = data;
			this.offset = offset;
		}

		public List<SecurityAce> getAces() {
			ByteBuffer buffer = ByteBuffer.wrap(data, offset, data.length - offset);
			buffer.getShort(); // Unused
			int size = buffer.getShort();
			int numAces = buffer.getInt();

			List<SecurityAce> aces = new ArrayList<>();
			int aceOffset = offset + size;

			for (int i = 0; i < numAces; i++) {
				int accessMask = buffer.getInt();
				Sid trustee = new Sid(data, aceOffset + 8);
				aces.add(new SecurityAce(accessMask, trustee));

				aceOffset += 8 + buffer.getShort();
			}

			return aces;
		}
	}

	static class SecurityDescriptor {
		private byte[] data;
		private int offset;

		private int ownerOffset;
		private int groupOffset;
		private int saclOffset;
		private int daclOffset;

		public SecurityDescriptor(byte[] data, int offset) {
			this.data = data;
			this.offset = offset;
			unpack();
		}

		private void unpack() {
			ByteBuffer buffer = ByteBuffer.wrap(data, offset + 4, 16);
			ownerOffset = buffer.getInt();
			groupOffset = buffer.getInt();
			saclOffset = buffer.getInt();
			daclOffset = buffer.getInt();
		}

		public Sid getOwnerSid() {
			return new Sid(data, ownerOffset);
		}

		public Sid getGroupSid() {
			return new Sid(data, groupOffset);
		}

		public SecurityAcl getDacl() {
			return daclOffset == 0 ? null : new SecurityAcl(data, daclOffset);
		}
	}

	static class NtAcl {
		private byte[] data;

		public NtAcl(String base64Data) {
			this.data = Base64.getDecoder().decode(base64Data);
		}

		public SecurityDescriptor getSecurityDescriptor() {
			ByteBuffer buffer = ByteBuffer.wrap(data);
			int version = buffer.getShort();

			int sdOffset = (version == 3) ? 0x50 : 0xA0;
			return new SecurityDescriptor(data, sdOffset);
		}

		public Map<String, Object> toMap() {
			Map<String, Object> ntAclMap = new HashMap<>();

			SecurityDescriptor sd = getSecurityDescriptor();
			ntAclMap.put("owner", sd.getOwnerSid().getName());
			ntAclMap.put("group", sd.getGroupSid().getName());
			List<Map<String, Object>> aclList = new ArrayList<>();
			SecurityAcl dacl = sd.getDacl();
			if (dacl != null) {
				for (SecurityAce ace : dacl.getAces()) {
					Map<String, Object> aceMap = new HashMap<>();
					aceMap.put("trustee", ace.getTrustee().getName());
					aceMap.put("permissions", ace.getPermissions());
					aceMap.put("access", ace.getAccess());
					aclList.add(aceMap);
				}
			}
			ntAclMap.put("acls", aclList);
			return ntAclMap;
		}
	}

	public static void main(String[] args) {
		String ntAclEncoded = "BAAEAAAAAgAEAAIAAQCKE3vvv5/JbYU9ZS5M/XeH069aURSj+wYh1S0cRgPpfQAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAcG9zaXhfYWNsAIxuOCNnUdsBdOCUnqsUzCOUJEiVBXKU7HGxzbItwA9Z7udBPXCDdrcAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAEABIy0AAAA0AAAAAAAAADsAAAAAQUAAAAAAAUVAAAApSX3c+BCm5wgHXdi9AEAAAEFAAAAAAAFFQAAAKUl93PgQpucIB13YgECAAADACwAAQAAAAAQJAD/AR8AAQUAAAAAAAUVAAAApSX3c+BCm5wgHXdiWgQAAA==";
		// Use your NT ACL value
		NtAcl ntAcl = new NtAcl(ntAclEncoded);
		System.out.println(ntAcl.toMap());
	}
}
