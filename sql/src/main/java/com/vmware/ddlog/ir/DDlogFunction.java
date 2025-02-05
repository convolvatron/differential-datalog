/*
 * Copyright (c) 2021 VMware, Inc.
 * SPDX-License-Identifier: MIT
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 *
 */

package com.vmware.ddlog.ir;

import com.facebook.presto.sql.tree.Node;
import com.vmware.ddlog.util.Linq;
import com.vmware.ddlog.util.Utilities;

import javax.annotation.Nullable;

public class DDlogFunction extends DDlogNode {
    final String name;
    final DDlogFuncArg[] args;
    final DDlogType type;
    @Nullable
    final DDlogExpression def;

    public DDlogFunction(@Nullable Node node, String name, DDlogType type, @Nullable DDlogExpression def, DDlogFuncArg... args) {
        super(node);
        this.name = name;
        this.args = args;
        this.type = type;
        this.def = def;
    }

    @Override
    public String toString() {
        String result = "";
        if (this.def == null)
            result = "extern ";
        result += "function " + this.name + "(" +
            String.join(", ", Linq.map(this.args, DDlogFuncArg::toString, String.class)) + "):" +
            this.type.toString();
        if (this.def != null)
            result += " {\n" + this.def.toString() + "\n}\n";
        return result;
    }

    public boolean compare(DDlogFunction other, IComparePolicy policy) {
        if (!policy.compareIdentifier(this.name, other.name))
            return false;
        if (this.args.length != other.args.length)
            return false;
        for (int i = 0; i < this.args.length; i++)
            if (!this.args[i].compare(other.args[i], policy))
                return false;
        if (!this.type.compare(other.type, policy))
            return false;
        switch (Utilities.canBeSame(this.def, other.def)) {
            case Yes:
                break;
            case No:
                return false;
            case Maybe:
                assert this.def != null;
                assert other.def != null;
                if (!this.def.compare(other.def, policy))
                    return false;
        }

        for (int i = 0; i < this.args.length; i++)
            policy.exitScope(this.args[i].name, other.args[i].name);
        return true;
    }
}
