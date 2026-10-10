# Legacy instrument restoration

The tagged backup writer fixes new backups, but older untyped dictionaries can
still overwrite initialized Assets during restoration. Reinstalling the current
runtime does not supply the lost type information.

The initialized variable tree provides a narrow type schema. Both scheduled-file
and database readers restore canonical legacy Asset payloads only at paths that
already contain an Asset. Ordinary metadata dictionaries and unknown paths stay
dictionaries. Saved instrument identity, quantities, signals and contracts remain
authoritative. All validation completes before modifying variables.

Scheduled readers preserve original bytes in a content-addressed, mode-0600
sibling file before applying a recovered tree. The existing state file is not
rewritten by loading. Hosts must separately retain the remote original; this local
copy is not a claim of durable S3 preservation. Database operators must preserve
the original record before allowing a migrated backup to replace it.

Regression tests cover both readers, nested stock and option fields, repeated
save/restart, ordinary Asset-shaped metadata, saved contracts differing from
initialized defaults, invalid payload atomicity, and original file preservation.
Unknown legacy paths require explicit operator-approved instrument conversion;
do not globally infer instruments from arbitrary ticker dictionaries.
