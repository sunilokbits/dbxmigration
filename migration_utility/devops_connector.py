"""Azure DevOps Git connector — push files to a repo without local git clone.

Uses the Azure DevOps REST API (Git Pushes) to atomically commit files
(DDL .sql + ER diagram PNG + optional model JSON) to a branch.
"""
import base64
from urllib.parse import quote
import requests
from log_config import get_logger

logger = get_logger(__name__)

_API_VERSION = "7.0"


def _headers(pat: str) -> dict:
    """Build auth headers using PAT (Basic auth with empty username)."""
    b64 = base64.b64encode(f":{pat}".encode()).decode()
    return {
        "Authorization": f"Basic {b64}",
        "Content-Type": "application/json",
    }


def _parse_org(org: str) -> str:
    """Normalise org input — strip /_git/... or /project/... suffixes.

    Users may paste a full clone URL like:
      https://dev.azure.com/EMEA-SalesOps/AI%20Accelerator/_git/AI%20Accelerator
    We only need the org base:  https://dev.azure.com/EMEA-SalesOps
    """
    org = org.strip().rstrip("/")
    if not org.startswith("http"):
        return f"https://dev.azure.com/{org}"
    # Strip anything after the org name in the URL
    # Pattern: https://dev.azure.com/{org}  or  https://{org}.visualstudio.com
    if "dev.azure.com" in org or "azure.com" in org:
        # Remove path segments after org name: /_git/*, /project/*
        parts = org.split("/")
        # https://dev.azure.com/OrgName  → 4 parts minimum
        # Keep only scheme + host + org
        if len(parts) > 4:
            org = "/".join(parts[:4])
    elif "visualstudio.com" in org:
        # https://org.visualstudio.com/...  → keep scheme + host only
        parts = org.split("/")
        if len(parts) > 3:
            org = "/".join(parts[:3])
    return org


def _api_url(org: str, project: str, repo: str, path: str) -> str:
    """Build Azure DevOps REST API URL with properly encoded components."""
    base = _parse_org(org)
    # URL-encode project and repo names (spaces, special chars)
    enc_project = quote(project.strip(), safe="")
    enc_repo = quote(repo.strip(), safe="")
    enc_path = quote(path.strip(), safe="/") if path else ""
    if enc_path:
        return f"{base}/{enc_project}/_apis/git/repositories/{enc_repo}/{enc_path}?api-version={_API_VERSION}"
    return f"{base}/{enc_project}/_apis/git/repositories/{enc_repo}?api-version={_API_VERSION}"


def _get_branch_ref(org: str, project: str, repo: str, branch: str, pat: str) -> str | None:
    """Get the objectId of a branch (returns None if branch doesn't exist)."""
    url = _api_url(org, project, repo, "refs")
    url += f"&filter=heads/{branch}"
    resp = requests.get(url, headers=_headers(pat), timeout=30)
    if resp.status_code != 200:
        return None
    refs = resp.json().get("value", [])
    for ref in refs:
        if ref.get("name") == f"refs/heads/{branch}":
            return ref.get("objectId")
    return None


def _get_default_branch_ref(org: str, project: str, repo: str, pat: str) -> tuple[str, str]:
    """Get the default branch name and objectId."""
    url = _api_url(org, project, repo, "")
    resp = requests.get(url, headers=_headers(pat), timeout=30)
    if resp.status_code != 200:
        raise RuntimeError(f"Cannot access repo: HTTP {resp.status_code} — {resp.text[:200]}")
    data = resp.json()
    default_branch = data.get("defaultBranch", "refs/heads/main")
    # Get the ref objectId
    branch_name = default_branch.replace("refs/heads/", "")
    ref_id = _get_branch_ref(org, project, repo, branch_name, pat)
    if not ref_id:
        raise RuntimeError(f"Cannot find default branch '{branch_name}' ref")
    return branch_name, ref_id


def push_files_to_repo(
    org: str,
    project: str,
    repo: str,
    branch: str,
    files: list[dict],
    commit_message: str,
    pat: str,
) -> dict:
    """Push files to an Azure DevOps Git repo in a single atomic commit.

    Args:
        org: Azure DevOps organization name or full URL
        project: Project name
        repo: Repository name
        branch: Target branch name (created from default if missing)
        files: List of dicts with keys: path, content, encoding
               encoding: "utf-8" for text, "base64" for binary
        commit_message: Commit message
        pat: Personal Access Token

    Returns:
        dict with commit_id, url, success
    """
    if not org or not project or not repo or not pat:
        raise ValueError("org, project, repo, and pat are all required")
    if not files:
        raise ValueError("No files to push")

    # Check if target branch exists
    branch_ref = _get_branch_ref(org, project, repo, branch, pat)

    if not branch_ref:
        # Create branch from default branch
        logger.info("Branch '%s' not found, creating from default branch", branch)
        _, default_ref = _get_default_branch_ref(org, project, repo, pat)
        # Create the branch via refs endpoint
        create_url = _api_url(org, project, repo, "refs")
        create_body = [{
            "name": f"refs/heads/{branch}",
            "oldObjectId": "0000000000000000000000000000000000000000",
            "newObjectId": default_ref,
        }]
        resp = requests.post(create_url, json=create_body, headers=_headers(pat), timeout=30)
        if resp.status_code not in (200, 201):
            raise RuntimeError(f"Failed to create branch '{branch}': {resp.status_code} — {resp.text[:300]}")
        branch_ref = default_ref
        logger.info("Created branch '%s' from default", branch)

    # Check which files already exist (for edit vs add)
    changes = []
    for f in files:
        file_path = f["path"].lstrip("/")
        # Check if file exists on this branch
        item_url = _api_url(org, project, repo, f"items")
        item_url += f"&path=/{file_path}&versionDescriptor.version={branch}&versionDescriptor.versionType=branch"
        item_resp = requests.get(item_url, headers=_headers(pat), timeout=15)
        change_type = "edit" if item_resp.status_code == 200 else "add"

        if f.get("encoding") == "base64":
            content_type = "base64Encoded"
            content = f["content"]
        else:
            content_type = "rawText"
            content = f["content"]

        changes.append({
            "changeType": change_type,
            "item": {"path": f"/{file_path}"},
            "newContent": {
                "content": content,
                "contentType": content_type,
            },
        })

    # Build push payload
    push_body = {
        "refUpdates": [{
            "name": f"refs/heads/{branch}",
            "oldObjectId": branch_ref,
        }],
        "commits": [{
            "comment": commit_message,
            "changes": changes,
        }],
    }

    push_url = _api_url(org, project, repo, "pushes")
    resp = requests.post(push_url, json=push_body, headers=_headers(pat), timeout=60)

    if resp.status_code in (200, 201):
        data = resp.json()
        commits = data.get("commits", [{}])
        commit_id = commits[0].get("commitId", "") if commits else ""
        # Build web URL to the commit
        org_clean = org.strip().rstrip("/")
        if not org_clean.startswith("http"):
            org_clean = f"https://dev.azure.com/{org_clean}"
        web_url = f"{org_clean}/{project}/_git/{repo}/commit/{commit_id}"
        logger.info("Successfully pushed %d files to %s/%s branch '%s'", len(files), project, repo, branch)
        return {"success": True, "commit_id": commit_id, "url": web_url, "files_pushed": len(files)}
    else:
        error_msg = resp.text[:500]
        logger.error("Push failed: HTTP %d — %s", resp.status_code, error_msg)
        raise RuntimeError(f"Push failed (HTTP {resp.status_code}): {error_msg}")


def _resolve_identity(org: str, pat: str, email: str) -> str | None:
    """Resolve an email/display name to an Azure DevOps identity GUID.

    Uses the Identity Picker API (used by the DevOps web UI itself).
    """
    base = _parse_org(org)
    url = f"{base}/_apis/IdentityPicker/Identities?api-version={_API_VERSION}"
    body = {
        "query": email,
        "identityTypes": ["user"],
        "operationScopes": ["ims", "source"],
        "properties": ["DisplayName", "Mail", "SubjectDescriptor"],
    }
    try:
        resp = requests.post(url, json=body, headers=_headers(pat), timeout=15)
        if resp.status_code == 200:
            results = resp.json().get("results", [])
            for result in results:
                identities = result.get("identities", [])
                for identity in identities:
                    # Match by mail or signInAddress
                    mail = (identity.get("mail") or "").lower()
                    sign_in = (identity.get("signInAddress") or "").lower()
                    if email.lower() in (mail, sign_in) or email.lower() in identity.get("displayName", "").lower():
                        local_id = identity.get("localId")
                        if local_id:
                            return local_id
            # If exact match not found, return the first result's localId
            if results and results[0].get("identities"):
                return results[0]["identities"][0].get("localId")
    except Exception as e:
        logger.warning("Identity resolution failed for '%s': %s", email, e)
    return None


def create_pull_request(
    org: str,
    project: str,
    repo: str,
    source_branch: str,
    target_branch: str,
    title: str,
    description: str,
    reviewers: list[str],
    pat: str,
    auto_complete: bool = False,
) -> dict:
    """Create a Pull Request in Azure DevOps with optional reviewers.

    Args:
        org: Azure DevOps organization name or full URL
        project: Project name
        repo: Repository name
        source_branch: Branch with the changes
        target_branch: Branch to merge into
        title: PR title
        description: PR description (markdown supported)
        reviewers: List of reviewer identifiers (email or Azure DevOps user ID)
        pat: Personal Access Token
        auto_complete: If True, set the PR to auto-complete when all policies pass

    Returns:
        dict with pr_id, web_url, success
    """
    url = _api_url(org, project, repo, "pullrequests")

    body = {
        "sourceRefName": f"refs/heads/{source_branch}",
        "targetRefName": f"refs/heads/{target_branch}",
        "title": title,
        "description": description,
        "reviewers": [],
    }

    # Resolve reviewer identities to GUIDs (Azure DevOps requires id, not email)
    unresolved_reviewers = []
    for reviewer in reviewers:
        reviewer = reviewer.strip()
        if not reviewer:
            continue
        # If it looks like a GUID, use as-is
        if len(reviewer) == 36 and reviewer.count("-") == 4:
            body["reviewers"].append({"id": reviewer})
        else:
            # Resolve email to identity GUID
            identity_id = _resolve_identity(org, pat, reviewer)
            if identity_id:
                body["reviewers"].append({"id": identity_id})
            else:
                unresolved_reviewers.append(reviewer)
                logger.warning("Could not resolve reviewer identity: '%s'", reviewer)

    # If we couldn't resolve any reviewers, still create the PR (branch policies will enforce)
    resp = requests.post(url, json=body, headers=_headers(pat), timeout=30)

    if resp.status_code in (200, 201):
        data = resp.json()
        pr_id = data.get("pullRequestId")
        org_clean = _parse_org(org)
        web_url = f"{org_clean}/{quote(project.strip(), safe='')}/_git/{quote(repo.strip(), safe='')}/pullrequest/{pr_id}"

        # Optionally enable auto-complete
        if auto_complete and pr_id:
            _set_auto_complete(org, project, repo, pr_id, data.get("createdBy", {}).get("id", ""), pat)

        logger.info("Created PR #%s: %s → %s in %s/%s", pr_id, source_branch, target_branch, project, repo)
        result = {
            "success": True,
            "pr_id": pr_id,
            "web_url": web_url,
            "status": data.get("status", "active"),
        }
        if unresolved_reviewers:
            result["warning"] = f"Could not resolve reviewers: {', '.join(unresolved_reviewers)}. PR created without them — branch policies will still apply."
        return result
    else:
        error_msg = resp.text[:500]
        logger.error("Create PR failed: HTTP %d — %s", resp.status_code, error_msg)
        raise RuntimeError(f"Failed to create PR (HTTP {resp.status_code}): {error_msg}")


def _set_auto_complete(org: str, project: str, repo: str, pr_id: int, creator_id: str, pat: str):
    """Set auto-complete on a PR so it merges automatically when policies pass."""
    url = _api_url(org, project, repo, f"pullrequests/{pr_id}")
    body = {
        "autoCompleteSetBy": {"id": creator_id},
        "completionOptions": {
            "mergeStrategy": "squash",
            "deleteSourceBranch": True,
            "transitionWorkItems": True,
        },
    }
    try:
        requests.patch(url, json=body, headers=_headers(pat), timeout=15)
    except Exception as e:
        logger.warning("Could not set auto-complete on PR #%s: %s", pr_id, e)


def get_pull_request_status(
    org: str, project: str, repo: str, pr_id: int, pat: str
) -> dict:
    """Get the status of a Pull Request."""
    url = _api_url(org, project, repo, f"pullrequests/{pr_id}")
    resp = requests.get(url, headers=_headers(pat), timeout=15)
    if resp.status_code == 200:
        data = resp.json()
        return {
            "success": True,
            "pr_id": pr_id,
            "status": data.get("status", "unknown"),
            "merge_status": data.get("mergeStatus", "unknown"),
            "title": data.get("title", ""),
            "reviewers": [
                {
                    "name": r.get("displayName", ""),
                    "vote": r.get("vote", 0),
                }
                for r in data.get("reviewers", [])
            ],
        }
    return {"success": False, "error": f"HTTP {resp.status_code}"}


def test_connection(org: str, project: str, repo: str, pat: str) -> dict:
    """Test connectivity to an Azure DevOps repo (read-only, no push).

    Returns dict with success, repo_name, default_branch.
    """
    if not org or not project or not repo or not pat:
        raise ValueError("org, project, repo, and pat are all required")

    url = _api_url(org, project, repo, "")
    resp = requests.get(url, headers=_headers(pat), timeout=30)

    if resp.status_code == 203:
        raise RuntimeError("Authentication failed — PAT is invalid or expired.")
    if resp.status_code == 401:
        raise RuntimeError("Authentication failed — PAT is invalid or expired (401).")
    if resp.status_code == 403:
        raise RuntimeError("Access denied — PAT does not have permission to this repo (403).")
    if resp.status_code == 404:
        raise RuntimeError(f"Repository '{repo}' not found in project '{project}'. Check org/project/repo names.")
    if resp.status_code != 200:
        raise RuntimeError(f"Cannot access repo: HTTP {resp.status_code} — {resp.text[:200]}")

    data = resp.json()
    repo_name = data.get("name", repo)
    default_branch = data.get("defaultBranch", "refs/heads/main").replace("refs/heads/", "")
    return {"success": True, "repo_name": repo_name, "default_branch": default_branch}
