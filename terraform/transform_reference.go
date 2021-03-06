package terraform

import (
	"fmt"
	"log"
	"sort"

	"github.com/hashicorp/hcl/v2"
	"github.com/hashicorp/terraform/addrs"
	"github.com/hashicorp/terraform/configs"
	"github.com/hashicorp/terraform/configs/configschema"
	"github.com/hashicorp/terraform/dag"
	"github.com/hashicorp/terraform/lang"
	"github.com/hashicorp/terraform/states"
)

// GraphNodeReferenceable must be implemented by any node that represents
// a Terraform thing that can be referenced (resource, module, etc.).
//
// Even if the thing has no name, this should return an empty list. By
// implementing this and returning a non-nil result, you say that this CAN
// be referenced and other methods of referencing may still be possible (such
// as by path!)
type GraphNodeReferenceable interface {
	GraphNodeSubPath

	// ReferenceableAddrs returns a list of addresses through which this can be
	// referenced.
	ReferenceableAddrs() []addrs.Referenceable
}

// GraphNodeReferencer must be implemented by nodes that reference other
// Terraform items and therefore depend on them.
type GraphNodeReferencer interface {
	GraphNodeSubPath

	// References returns a list of references made by this node, which
	// include both a referenced address and source location information for
	// the reference.
	References() []*addrs.Reference
}

type GraphNodeAttachDependencies interface {
	GraphNodeResource
	AttachDependencies([]addrs.AbsResource)
}

// GraphNodeReferenceOutside is an interface that can optionally be implemented.
// A node that implements it can specify that its own referenceable addresses
// and/or the addresses it references are in a different module than the
// node itself.
//
// Any referenceable addresses returned by ReferenceableAddrs are interpreted
// relative to the returned selfPath.
//
// Any references returned by References are interpreted relative to the
// returned referencePath.
//
// It is valid but not required for either of these paths to match what is
// returned by method Path, though if both match the main Path then there
// is no reason to implement this method.
//
// The primary use-case for this is the nodes representing module input
// variables, since their expressions are resolved in terms of their calling
// module, but they are still referenced from their own module.
type GraphNodeReferenceOutside interface {
	// ReferenceOutside returns a path in which any references from this node
	// are resolved.
	ReferenceOutside() (selfPath, referencePath addrs.ModuleInstance)
}

// ReferenceTransformer is a GraphTransformer that connects all the
// nodes that reference each other in order to form the proper ordering.
type ReferenceTransformer struct{}

func (t *ReferenceTransformer) Transform(g *Graph) error {
	// Build a reference map so we can efficiently look up the references
	vs := g.Vertices()
	m := NewReferenceMap(vs)

	// Find the things that reference things and connect them
	for _, v := range vs {
		if _, ok := v.(GraphNodeDestroyer); ok {
			// destroy nodes references are not connected, since they can only
			// use their own state.
			continue
		}

		parents, _ := m.References(v)
		parentsDbg := make([]string, len(parents))
		for i, v := range parents {
			parentsDbg[i] = dag.VertexName(v)
		}
		log.Printf(
			"[DEBUG] ReferenceTransformer: %q references: %v",
			dag.VertexName(v), parentsDbg)

		for _, parent := range parents {
			g.Connect(dag.BasicEdge(v, parent))
		}

		if len(parents) > 0 {
			continue
		}
	}

	return nil
}

// AttachDependenciesTransformer records all resource dependencies for each
// instance, and attaches the addresses to the node itself. Managed resource
// will record these in the state for proper ordering of destroy operations.
type AttachDependenciesTransformer struct {
	Config  *configs.Config
	State   *states.State
	Schemas *Schemas
}

func (t AttachDependenciesTransformer) Transform(g *Graph) error {
	for _, v := range g.Vertices() {
		attacher, ok := v.(GraphNodeAttachDependencies)
		if !ok {
			continue
		}
		selfAddr := attacher.ResourceAddr()

		// Data sources don't need to track destroy dependencies
		if selfAddr.Resource.Mode == addrs.DataResourceMode {
			continue
		}

		ans, err := g.Ancestors(v)
		if err != nil {
			return err
		}

		// dedupe addrs when there's multiple instances involved, or
		// multiple paths in the un-reduced graph
		depMap := map[string]addrs.AbsResource{}
		for _, d := range ans {
			var addr addrs.AbsResource

			switch d := d.(type) {
			case GraphNodeResourceInstance:
				instAddr := d.ResourceInstanceAddr()
				addr = instAddr.Resource.Resource.Absolute(instAddr.Module)
			case GraphNodeResource:
				addr = d.ResourceAddr()
			default:
				continue
			}

			// Data sources don't need to track destroy dependencies
			if addr.Resource.Mode == addrs.DataResourceMode {
				continue
			}

			if addr.Equal(selfAddr) {
				continue
			}
			depMap[addr.String()] = addr
		}

		deps := make([]addrs.AbsResource, 0, len(depMap))
		for _, d := range depMap {
			deps = append(deps, d)
		}
		sort.Slice(deps, func(i, j int) bool {
			return deps[i].String() < deps[j].String()
		})

		log.Printf("[TRACE] AttachDependenciesTransformer: %s depends on %s", attacher.ResourceAddr(), deps)
		attacher.AttachDependencies(deps)
	}

	return nil
}

// PruneUnusedValuesTransformer is a GraphTransformer that removes local,
// variable, and output values which are not referenced in the graph. If these
// values reference a resource that is no longer in the state the interpolation
// could fail.
type PruneUnusedValuesTransformer struct {
	Destroy bool
}

func (t *PruneUnusedValuesTransformer) Transform(g *Graph) error {
	// Pruning a value can effect previously checked edges, so loop until there
	// are no more changes.
	for removed := 0; ; removed = 0 {
		for _, v := range g.Vertices() {
			switch v := v.(type) {
			case *NodeApplyableOutput:
				// If we're not certain this is a full destroy, we need to keep any
				// root module outputs
				if v.Addr.Module.IsRoot() && !t.Destroy {
					continue
				}
			case *NodeLocal, *NodeApplyableModuleVariable:
				// OK
			default:
				// We're only concerned with variables, locals and outputs
				continue
			}

			dependants := g.UpEdges(v)

			switch dependants.Len() {
			case 0:
				// nothing at all depends on this
				log.Printf("[TRACE] PruneUnusedValuesTransformer: removing unused value %s", dag.VertexName(v))
				g.Remove(v)
				removed++
			case 1:
				// because an output's destroy node always depends on the output,
				// we need to check for the case of a single destroy node.
				d := dependants.List()[0]
				if _, ok := d.(*NodeDestroyableOutput); ok {
					log.Printf("[TRACE] PruneUnusedValuesTransformer: removing unused value %s", dag.VertexName(v))
					g.Remove(v)
					removed++
				}
			}
		}
		if removed == 0 {
			break
		}
	}

	return nil
}

// ReferenceMap is a structure that can be used to efficiently check
// for references on a graph.
type ReferenceMap struct {
	// vertices is a map from internal reference keys (as produced by the
	// mapKey method) to one or more vertices that are identified by each key.
	//
	// A particular reference key might actually identify multiple vertices,
	// e.g. in situations where one object is contained inside another.
	vertices map[string][]dag.Vertex

	// edges is a map whose keys are a subset of the internal reference keys
	// from "vertices", and whose values are the nodes that refer to each
	// key. The values in this map are the referrers, while values in
	// "verticies" are the referents. The keys in both cases are referents.
	edges map[string][]dag.Vertex
}

// References returns the set of vertices that the given vertex refers to,
// and any referenced addresses that do not have corresponding vertices.
func (m *ReferenceMap) References(v dag.Vertex) ([]dag.Vertex, []addrs.Referenceable) {
	rn, ok := v.(GraphNodeReferencer)
	if !ok {
		return nil, nil
	}
	if _, ok := v.(GraphNodeSubPath); !ok {
		return nil, nil
	}

	var matches []dag.Vertex
	var missing []addrs.Referenceable

	for _, ref := range rn.References() {
		subject := ref.Subject

		key := m.referenceMapKey(v, subject)
		if _, exists := m.vertices[key]; !exists {
			// If what we were looking for was a ResourceInstance then we
			// might be in a resource-oriented graph rather than an
			// instance-oriented graph, and so we'll see if we have the
			// resource itself instead.
			switch ri := subject.(type) {
			case addrs.ResourceInstance:
				subject = ri.ContainingResource()
			case addrs.ResourceInstancePhase:
				subject = ri.ContainingResource()
			}
			key = m.referenceMapKey(v, subject)
		}

		vertices := m.vertices[key]
		for _, rv := range vertices {
			// don't include self-references
			if rv == v {
				continue
			}
			matches = append(matches, rv)
		}
		if len(vertices) == 0 {
			missing = append(missing, ref.Subject)
		}
	}

	return matches, missing
}

// Referrers returns the set of vertices that refer to the given vertex.
func (m *ReferenceMap) Referrers(v dag.Vertex) []dag.Vertex {
	rn, ok := v.(GraphNodeReferenceable)
	if !ok {
		return nil
	}
	sp, ok := v.(GraphNodeSubPath)
	if !ok {
		return nil
	}

	var matches []dag.Vertex
	for _, addr := range rn.ReferenceableAddrs() {
		key := m.mapKey(sp.Path(), addr)
		referrers, ok := m.edges[key]
		if !ok {
			continue
		}

		// If the referrer set includes our own given vertex then we skip,
		// since we don't want to return self-references.
		selfRef := false
		for _, p := range referrers {
			if p == v {
				selfRef = true
				break
			}
		}
		if selfRef {
			continue
		}

		matches = append(matches, referrers...)
	}

	return matches
}

func (m *ReferenceMap) mapKey(path addrs.ModuleInstance, addr addrs.Referenceable) string {
	return fmt.Sprintf("%s|%s", path.String(), addr.String())
}

// vertexReferenceablePath returns the path in which the given vertex can be
// referenced. This is the path that its results from ReferenceableAddrs
// are considered to be relative to.
//
// Only GraphNodeSubPath implementations can be referenced, so this method will
// panic if the given vertex does not implement that interface.
func (m *ReferenceMap) vertexReferenceablePath(v dag.Vertex) addrs.ModuleInstance {
	sp, ok := v.(GraphNodeSubPath)
	if !ok {
		// Only nodes with paths can participate in a reference map.
		panic(fmt.Errorf("vertexMapKey on vertex type %T which doesn't implement GraphNodeSubPath", sp))
	}

	if outside, ok := v.(GraphNodeReferenceOutside); ok {
		// Vertex is referenced from a different module than where it was
		// declared.
		path, _ := outside.ReferenceOutside()
		return path
	}

	// Vertex is referenced from the same module as where it was declared.
	return sp.Path()
}

// vertexReferencePath returns the path in which references _from_ the given
// vertex must be interpreted.
//
// Only GraphNodeSubPath implementations can have references, so this method
// will panic if the given vertex does not implement that interface.
func vertexReferencePath(referrer dag.Vertex) addrs.ModuleInstance {
	sp, ok := referrer.(GraphNodeSubPath)
	if !ok {
		// Only nodes with paths can participate in a reference map.
		panic(fmt.Errorf("vertexReferencePath on vertex type %T which doesn't implement GraphNodeSubPath", sp))
	}

	var path addrs.ModuleInstance
	if outside, ok := referrer.(GraphNodeReferenceOutside); ok {
		// Vertex makes references to objects in a different module than where
		// it was declared.
		_, path = outside.ReferenceOutside()
		return path
	}

	// Vertex makes references to objects in the same module as where it
	// was declared.
	return sp.Path()
}

// referenceMapKey produces keys for the "edges" map. "referrer" is the vertex
// that the reference is from, and "addr" is the address of the object being
// referenced.
//
// The result is an opaque string that includes both the address of the given
// object and the address of the module instance that object belongs to.
//
// Only GraphNodeSubPath implementations can be referrers, so this method will
// panic if the given vertex does not implement that interface.
func (m *ReferenceMap) referenceMapKey(referrer dag.Vertex, addr addrs.Referenceable) string {
	path := vertexReferencePath(referrer)
	return m.mapKey(path, addr)
}

// NewReferenceMap is used to create a new reference map for the
// given set of vertices.
func NewReferenceMap(vs []dag.Vertex) *ReferenceMap {
	var m ReferenceMap

	// Build the lookup table
	vertices := make(map[string][]dag.Vertex)
	for _, v := range vs {
		_, ok := v.(GraphNodeSubPath)
		if !ok {
			// Only nodes with paths can participate in a reference map.
			continue
		}

		// We're only looking for referenceable nodes
		rn, ok := v.(GraphNodeReferenceable)
		if !ok {
			continue
		}

		path := m.vertexReferenceablePath(v)

		// Go through and cache them
		for _, addr := range rn.ReferenceableAddrs() {
			key := m.mapKey(path, addr)
			vertices[key] = append(vertices[key], v)
		}

		// Any node can be referenced by the address of the module it belongs
		// to or any of that module's ancestors.
		for _, addr := range path.Ancestors()[1:] {
			// Can be referenced either as the specific call instance (with
			// an instance key) or as the bare module call itself (the "module"
			// block in the parent module that created the instance).
			callPath, call := addr.Call()
			callInstPath, callInst := addr.CallInstance()
			callKey := m.mapKey(callPath, call)
			callInstKey := m.mapKey(callInstPath, callInst)
			vertices[callKey] = append(vertices[callKey], v)
			vertices[callInstKey] = append(vertices[callInstKey], v)
		}
	}

	// Build the lookup table for referenced by
	edges := make(map[string][]dag.Vertex)
	for _, v := range vs {
		_, ok := v.(GraphNodeSubPath)
		if !ok {
			// Only nodes with paths can participate in a reference map.
			continue
		}

		rn, ok := v.(GraphNodeReferencer)
		if !ok {
			// We're only looking for referenceable nodes
			continue
		}

		// Go through and cache them
		for _, ref := range rn.References() {
			if ref.Subject == nil {
				// Should never happen
				panic(fmt.Sprintf("%T.References returned reference with nil subject", rn))
			}
			key := m.referenceMapKey(v, ref.Subject)
			edges[key] = append(edges[key], v)
		}
	}

	m.vertices = vertices
	m.edges = edges
	return &m
}

// ReferencesFromConfig returns the references that a configuration has
// based on the interpolated variables in a configuration.
func ReferencesFromConfig(body hcl.Body, schema *configschema.Block) []*addrs.Reference {
	if body == nil {
		return nil
	}
	refs, _ := lang.ReferencesInBlock(body, schema)
	return refs
}

// appendResourceDestroyReferences identifies resource and resource instance
// references in the given slice and appends to it the "destroy-phase"
// equivalents of those references, returning the result.
//
// This can be used in the References implementation for a node which must also
// depend on the destruction of anything it references.
func appendResourceDestroyReferences(refs []*addrs.Reference) []*addrs.Reference {
	given := refs
	for _, ref := range given {
		switch tr := ref.Subject.(type) {
		case addrs.Resource:
			newRef := *ref // shallow copy
			newRef.Subject = tr.Phase(addrs.ResourceInstancePhaseDestroy)
			refs = append(refs, &newRef)
		case addrs.ResourceInstance:
			newRef := *ref // shallow copy
			newRef.Subject = tr.Phase(addrs.ResourceInstancePhaseDestroy)
			refs = append(refs, &newRef)
		}
	}
	return refs
}

func modulePrefixStr(p addrs.ModuleInstance) string {
	return p.String()
}

func modulePrefixList(result []string, prefix string) []string {
	if prefix != "" {
		for i, v := range result {
			result[i] = fmt.Sprintf("%s.%s", prefix, v)
		}
	}

	return result
}
