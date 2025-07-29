import os
import json
import subprocess
from app.process_manager import ProcessManager
from app.neon import NeonAPI

class HAProxyManager(ProcessManager):
    def __init__(self):
        super().__init__()
        self.haproxy_process = None
        self.neon_api = NeonAPI()

    def prepare_config(self):
        params = None
        
        if self.branch_id:
            try:
                params = self.neon_api.get_branch_connection_info(self.project_id, self.branch_id)
            except Exception as e:
                print(f"Debug: Error getting connection info: {str(e)}")
                raise
        elif self.parent_branch_id:
            state = self._get_neon_branch()
            current_branch = self._get_git_branch()
            parent = os.getenv("PARENT_BRANCH_ID")
            if parent == "":
                parent = None
            params, updated_state = self.neon_api.fetch_or_create_branch(state, current_branch, parent, self.vscode)
            self._write_neon_branch(updated_state)

        else:
            state = self._get_neon_branch()
            current_branch = self._get_git_branch()
            params, updated_state = self.neon_api.fetch_or_create_branch(state, current_branch, vscode=self.vscode)
            self._write_neon_branch(updated_state)
        
        if params is None:
            raise ValueError("Failed to get connection parameters")
            
        self._write_haproxy_config(params)

    def start_process(self):
        self.prepare_config()
        with open("/var/log/haproxy.log", "a") as log:
            self.haproxy_process = subprocess.Popen([
                "haproxy", "-f", "/tmp/haproxy.cfg"
            ], stdout=log, stderr=log)
        print("Neon Local is ready")

    def stop_process(self):
        if self.haproxy_process:
            print("Stopping HAProxy...")
            self.haproxy_process.terminate()
            try:
                self.haproxy_process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self.haproxy_process.kill()
                self.haproxy_process.wait()
            self.haproxy_process = None

    def _write_haproxy_config(self, databases):
        template_path = "/scripts/app/haproxy.cfg.tmpl"
        if not os.path.exists(template_path):
            raise FileNotFoundError(f"HAProxy config template not found at: {template_path}")

        with open(template_path, "r") as file:
            haproxy_template = file.read()

        print(f"Databases: {databases}")
        
        # Print database entries without sensitive info
        print("=== Database Entries (sanitized) ===")
        for i, db in enumerate(databases):
            print(f"Database {i+1}: {db['database']} -> {db['host']}:443 (user: ***, password: ***)")
        print("=== Full Database Entries ===")
        
        # Determine application name and user agent suffix based on CLIENT environment variable
        client = os.getenv("CLIENT", "").lower()
        app_name = "neon_local_vscode_container" if client == "vscode" else "neon_local_container"
        user_agent_suffix = "_neon_local_vscode_container" if client == "vscode" else "_neon_local_container"
        
        # Split the template into sections
        sections = haproxy_template.split("backend http_backend")
        frontend_section = sections[0].strip()
        backend_template = sections[1].strip()
        
        # Add global ACLs first
        frontend_section += "\n    acl is_sql path_beg /sql"
        
        # Generate backend sections for each database
        backend_sections = []
        for db in databases:
            backend_name = f"backend_{db['database']}"
            # Create backend section with proper structure
            backend_config = f"""
backend {backend_name}
    server ws_server1 {db['host']}:443 ssl verify none sni str({db['host']}) check
    http-request set-header Neon-Connection-String "postgresql://{db['user']}:{db['password']}@{db['host']}/{db['database']}?sslmode=require&application_name={app_name}"
    http-request set-header Host {db['host']}
    http-request set-header User-Agent "%[req.hdr(User-Agent)]{user_agent_suffix}"
"""
            backend_sections.append(backend_config)
            
            # Add database-specific ACLs and rules
            frontend_section += f"""
    acl is_{db['database']} path_beg /{db['database']}
    acl is_{db['database']}_connection hdr(Neon-Connection-String) -m reg -i {db['database']}
    use_backend {backend_name} if is_{db['database']} or is_sql is_{db['database']}_connection"""
        
        # Add default backend rule using the first database
        if databases:
            first_db = databases[0]
            default_backend = f"backend_{first_db['database']}"
            frontend_section += f"\n    default_backend {default_backend}"
        
        # Combine all sections with proper spacing
        haproxy_config = frontend_section + "\n\n" + "\n".join(backend_sections) + "\n"

        with open("/tmp/haproxy.cfg", "w") as file:
            file.write(haproxy_config)
