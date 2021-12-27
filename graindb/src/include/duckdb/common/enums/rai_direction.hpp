#pragma once

#include "duckdb/common/constants.hpp"

namespace duckdb {

enum class RAIDirection : uint8_t { DIRECTED, UNDIRECTED, SELF, PKFK };

static string GetRAIDirectionName(const RAIDirection &direction) {
	switch (direction) {
	case RAIDirection::DIRECTED: {
		return "DIRECTED";
	}
	case RAIDirection::UNDIRECTED: {
		return "UNDIRECTED";
	}
	case RAIDirection::SELF: {
		return "SELF";
	}
	case RAIDirection::PKFK: {
		return "PKFK";
	}
	}
}

} // namespace duckdb
